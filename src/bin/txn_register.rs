use anyhow::Context;
use crossbeam::channel::unbounded;
use log::LevelFilter;
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
    Config,
};
use maelstrom_convenience::{
    main_loop,
    raft::{
        Log, Raft, RaftData, State, Stateful, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL,
        MIN_REPLICATION_INTERVAL,
    },
    Body, Event, Message, Node,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::StdoutLock,
    thread,
    time::{Duration, Instant},
};

const READ: &str = "r";
const WRITE: &str = "w";
const NANO: f32 = 1_000_000_000.0;

pub type Command = (String, u32, Option<u32>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Error {
        code: u64,
        text: String,
    },
    Txn {
        txn: Vec<Command>,
    },
    TxnOk {
        txn: Vec<Command>,
    },
    RequestVote {
        term: u32,
        candidate: String,
        last_log_index: u32,
        last_log_term: u32,
    },
    RequestVoteOk {
        term: u32,
        vote_granted: bool,
    },
    AppendEntries {
        term: u32,
        leader_id: String,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<(u32, Option<LogItem>)>,
        leader_commit: u32,
    },
    AppendEntriesOk {
        term: u32,
        success: bool,
        index_plus_commited: u32,
    },
}

enum InternalCommunication {
    RunCandidate,
    StepDown,
    Replicate,
}

/// Injected payloads are payloads that we send to ourself and have to handle
/// separately
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    Req,
    Rep,
}

struct KVNode {
    id: usize,
    node: String,
    nodes: HashSet<String>,
    store: HashMap<u32, u32>,
    buffered: VecDeque<(String, u32, Vec<Command>)>,
    raft_data: RaftData<LogItem>,
    injector: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, InjectedPayload>>,
    comm: crossbeam::channel::Receiver<InternalCommunication>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LogItem {
    from: String,
    msg_id: u32,
    cmd: Command,
}

impl Stateful for KVNode {
    // #[inline(always)]
    fn state(&self) -> State {
        self.raft_data.state
    }

    // #[inline(always)]
    fn set_state(&mut self, state: State) {
        self.raft_data.state = state
    }
}

impl Raft for KVNode {
    type LogItem = LogItem;
    type Packet = Payload;

    fn advance_commit_index(&mut self) -> Vec<(String, u32, Payload)>{
        if self.state() == State::Leader {
            let med = median(self.raft_data.match_index.values().copied().collect()).unwrap_or(0);

            match self.raft_data.log.at(med as usize) {
                Some((term, _))
                    if self.raft_data.commit_index < med && term == self.get_current_term() =>
                {
                    log::info!("{}", format!("Commit index now: {}", med));
                    self.raft_data.commit_index = med;
                }
                _ => {}
            }
        }
        self.advance_map_from_log();
        vec![]
    }

    // #[inline(always)]
    fn get_current_term(&self) -> u32 {
        self.raft_data.term
    }

    // #[inline(always)]
    fn vote_for(&mut self, vote: String) {
        self.raft_data.voted_for = Some(vote);
    }

    fn reset_election_deadline(&mut self) {
        let mut rng = rand::thread_rng();
        let new_timeline = Instant::now().checked_add(Duration::from_secs_f32(
            rng.gen_range(0.0..ELECTION_TIMEOUT.as_secs_f32()),
        ));
        if let Some(new_timeline) = new_timeline {
            self.raft_data.election_deadline = new_timeline;
        }
    }

    fn reset_stepdown_deadline(&mut self) {
        self.raft_data.stepdown_deadline = Instant::now() + ELECTION_TIMEOUT;
    }

    // #[inline(always)]
    fn advance_term(&mut self, term: u32) {
        assert!(self.raft_data.term < term);
        self.raft_data.term = term;
        self.raft_data.voted_for = None;
    }

    fn request_votes(&mut self) {
        self.raft_data.votes.clear();
        self.raft_data.votes.insert(self.node.clone());
        log::info!("{}", format!("Cleared earlier votes"));
        self.injector
            .send(Event::Injected(InjectedPayload::Req))
            .expect("This has to be handled");
    }

    fn handle_vote_request(
        &mut self,
        remote_term: u32,
        candidate: String,
        last_log_index: u32,
        last_log_term: u32,
    ) -> (u32, bool) {
        self.maybe_step_down(remote_term);
        let mut grant = false;
        let our_term = self.get_current_term();

        if remote_term < self.get_current_term() {
            log::info!(
                "{}",
                format!(
                    "Candidate term {} lower than {}, not granting vote.",
                    remote_term, our_term
                )
            );
        } else if let Some(vote) = self.raft_data.voted_for.as_ref() {
            log::info!(
                "{}",
                format!("Already voted for {}; not granting vote.", vote)
            );
        } else if last_log_term < self.raft_data.log.last().0 {
            log::info!(
                "{}",
                format!(
                    "Have log entries from term {}, which is ne
                    wer than remote term {}; not granting vote.",
                    self.raft_data.log.last().0,
                    last_log_term
                )
            );
        } else if last_log_term == self.raft_data.log.last().0
            && last_log_index < self.raft_data.log.size() as u32
        {
            log::info!(
                "{}",
                format!(
                    "Our logs are both at term {}, but our log i
                    s {} and theirs is only {} long; not granting vo
                    te.",
                    last_log_term,
                    self.raft_data.log.size(),
                    last_log_index
                )
            );
        } else {
            log::info!("{}", format!("Granting vote to {}", candidate));
            grant = true;
            self.raft_data.voted_for = Some(candidate);
            self.reset_election_deadline();
        }

        (self.get_current_term(), grant)
    }

    fn handle_vote(&mut self, from: String, remote_term: u32, granted: bool) {
        self.reset_stepdown_deadline();
        self.maybe_step_down(remote_term);
        if self.raft_data.state == State::Candidate
            && self.raft_data.term == remote_term
            && self.raft_data.term == self.raft_data.term_vote_started
            && granted
        {
            self.raft_data.votes.insert(from);
            if self.raft_data.votes.len() >= majority(self.nodes.len()) {
                self.become_leader();
            }
        }
    }

    fn match_index(&mut self) {
        self.raft_data
            .match_index
            .insert(self.node.clone(), self.raft_data.log.size() as u32);
    }

    fn reset_match_index(&mut self) {
        self.raft_data.match_index.clear();
    }

    fn reset_next_index(&mut self) {}

    fn reset_last_replication(&mut self) {
        self.raft_data.last_replication = None;
    }

    fn update_indices(&mut self) {
        for node in &self.nodes {
            self.raft_data.match_index.insert(node.clone(), 0);
            self.raft_data
                .next_index
                .insert(node.clone(), self.raft_data.log.size() as u32 + 1);
        }
    }

    fn handle_append(
        &mut self,
        term: u32,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: &Vec<(u32, Option<LogItem>)>,
        leader_commit: u32,
    ) -> Self::Packet {
        self.maybe_step_down(term);
        let current_term = self.get_current_term();
        let index_plus_commited = prev_log_index + 1 + entries.len() as u32;
        let mut reply = Payload::AppendEntriesOk {
            term: current_term,
            success: false,
            // index plus commited is basically me keeping the log index per node stored on the "network"
            index_plus_commited,
        };

        if term < self.get_current_term() {
            return reply;
        }

        self.reset_election_deadline();

        if prev_log_index <= 0 {
            return Payload::Error {
                code: 0,
                text: String::from("index out of bounds"),
            };
        }

        let log_entry = self.raft_data.log.at(prev_log_index as usize);

        match log_entry {
            Some((term, _)) if term == prev_log_term => {
                self.raft_data.log.truncate(prev_log_index as usize);
                self.raft_data.log.append_all(entries.to_owned());
                if self.raft_data.commit_index < leader_commit {
                    self.raft_data.commit_index =
                        u32::min(self.raft_data.log.size() as u32, leader_commit);
                    self.advance_map_from_log();
                }
                reply = Payload::AppendEntriesOk {
                    term: current_term,
                    success: true,
                    index_plus_commited,
                };
                return reply;
            }
            _ => {
                return reply;
            }
        }
    }

    fn reset_presumed_leader(&mut self) {
        self.raft_data.presumed_leader = None;
    }
}

impl Node<(), Payload, InjectedPayload> for KVNode {
    fn from_init(
        _state: (),
        init: maelstrom_convenience::Init,
        inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let mut pattern = format!("{}", init.node_id.clone());
        pattern.push_str(" - {l} - {m}\n");
        let logfile = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(pattern.as_str())))
            .build("/Users/colinmikolajczak/Code/flyio/flyio-challenge/log/output.log")?;

        let config = Config::builder()
            .appender(Appender::builder().build("logfile", Box::new(logfile)))
            .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

        log4rs::init_config(config)?;

        let (s, r) = unbounded();
        let s1 = s.clone();
        let s2 = s.clone();
        thread::Builder::new()
            .name("RunCandidateThread".to_string())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs_f32(0.1));
                let rand_sleep = rand::thread_rng().gen_range(0.0..0.1);
                thread::sleep(Duration::new(0, (rand_sleep * NANO) as u32));
                let _ = s1.send(InternalCommunication::RunCandidate);
            })?;

        thread::Builder::new()
            .name("StepDownThread".to_string())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs_f32(0.1));
                let _ = s2.send(InternalCommunication::StepDown);
            })?;

        thread::Builder::new()
            .name("ReplicationThread".to_string())
            .spawn(move || loop {
                thread::sleep(MIN_REPLICATION_INTERVAL);
                let _ = s.send(InternalCommunication::Replicate);
            })?;

        let nodes = init
            .node_ids
            .into_iter()
            .filter(|node| *node != init.node_id)
            .collect();

        Ok(KVNode {
            id: 1,
            node: init.node_id,
            nodes,
            raft_data: RaftData {
                last_applied: 1,
                presumed_leader: None,
                voted_for: None,
                term: 0,
                term_vote_started: 0,
                state: State::Follower,
                votes: HashSet::new(),
                log: Log::new(),
                stepdown_deadline: Instant::now(),
                election_deadline: Instant::now(),
                last_replication: Some(Instant::now()),
                commit_index: 0,
                next_index: HashMap::default(),
                match_index: HashMap::default(),
            },
            store: HashMap::default(),
            comm: r,
            injector: inject.clone(),
            buffered: VecDeque::default(),
        })
    }

    fn step(
        &mut self,
        input: maelstrom_convenience::Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match self.comm.try_recv() {
            Ok(InternalCommunication::RunCandidate) => {
                if self.raft_data.election_deadline < Instant::now() {
                    if self.state() == State::Leader {
                        self.reset_election_deadline();
                    } else {
                        self.become_candidate(self.node.clone());
                    }
                }
            }
            Ok(InternalCommunication::StepDown) => {
                if self.raft_data.state == State::Leader
                    && self.raft_data.stepdown_deadline < Instant::now()
                {
                    log::info!("Stepping down: haven't received any acks lately");
                    self.become_follower();
                }
            }
            Ok(InternalCommunication::Replicate) => {
                self.injector.send(Event::Injected(InjectedPayload::Rep))?;
            }
            _ => {}
        }

        // check if we have unhandled txn messages in our buffer
        while !self.buffered.is_empty() && self.are_productive() {
            match self.buffered.pop_back() {
                Some(unhandled) => {
                    log::info!("{}", format!("Handling {:?} from buffer", &unhandled.2));
                    let p = self.handle_txn(
                        &unhandled.2,
                        unhandled.0.clone(),
                        Some(unhandled.1 as usize),
                        output,
                    )?;
                    if let Some(payload) = p {
                        Message {
                            src: self.node.clone(),
                            dst: unhandled.0,
                            body: Body {
                                id: Some(unhandled.1 as usize + 1),
                                in_reply_to: Some(unhandled.1 as usize + 1),
                                payload,
                            },
                        }
                        .send(output)
                        .with_context(|| {
                            format!("handling buffered txn failed on sending answer")
                        })?;
                    }
                }
                None => {}
            }
            if self.buffered.is_empty() {
                log::info!("Emptied internal buffer");
            }
        }

        match input {
            Event::Message(message) => {
                let id = message.body.id;
                let from = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                reply.body.in_reply_to = id;
                let new_payload = match reply.body.payload {
                    Payload::Txn { ref txn } => self.handle_txn(txn, from, id, output)?,
                    Payload::RequestVoteOk { term, vote_granted } => {
                        self.handle_vote(from, term, vote_granted);
                        None
                    }

                    Payload::RequestVote {
                        term,
                        ref candidate,
                        last_log_index,
                        last_log_term,
                    } => {
                        let (term, vote_granted) = self.handle_vote_request(
                            term,
                            candidate.clone(),
                            last_log_index,
                            last_log_term,
                        );
                        Some(Payload::RequestVoteOk { term, vote_granted })
                    }

                    Payload::AppendEntries {
                        term,
                        ref leader_id,
                        prev_log_index,
                        prev_log_term,
                        ref entries,
                        leader_commit,
                    } => {
                        let reply_payload = self.handle_append(
                            term,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        );
                        self.raft_data.presumed_leader.replace(leader_id.to_owned());
                        Some(reply_payload)
                    }

                    Payload::AppendEntriesOk {
                        term,
                        success,
                        index_plus_commited,
                    } => {
                        self.maybe_step_down(term);
                        if self.state() == State::Leader && term == self.get_current_term() {
                            self.reset_stepdown_deadline();
                            if success {
                                log::info!("{}", "replication succesful");
                                let next_index = *self.raft_data.next_index.get(&from).unwrap();
                                let match_index = *self.raft_data.match_index.get(&from).unwrap();
                                *self.raft_data.next_index.get_mut(&from).unwrap() =
                                    u32::max(next_index, index_plus_commited);
                                *self.raft_data.match_index.get_mut(&from).unwrap() =
                                    u32::max(match_index, index_plus_commited - 1);
                                log::info!(
                                    "{}",
                                    format!("Next index: {:?}", self.raft_data.next_index)
                                );
                                self.advance_commit_index();
                            } else {
                                *self.raft_data.next_index.get_mut(&from).unwrap() -= 1;
                            }
                        }
                        None
                    }

                    _ => None,
                };
                if let Some(new_payload) = new_payload {
                    KVNode::set_payload_and_send(&mut reply, new_payload, output)?
                }
            }
            Event::Injected(injected) => match injected {
                InjectedPayload::Req => {
                    let term = self.get_current_term();
                    let payload = Payload::RequestVote {
                        term,
                        candidate: self.node.clone(),
                        last_log_index: self.raft_data.log.size() as u32,
                        last_log_term: self.raft_data.log.last().0,
                    };
                    self.raft_data.term_vote_started = term;
                    self.broadcast(payload, output)?
                }
                InjectedPayload::Rep => {
                    let elapsed_time = self
                        .raft_data
                        .last_replication
                        .unwrap_or(Instant::now().checked_sub(Duration::from_secs(60)).unwrap())
                        .elapsed();
                    let mut replicated = false;

                    if self.state() == State::Leader && MIN_REPLICATION_INTERVAL < elapsed_time {
                        for node in &self.nodes {
                            let ni = *self.raft_data.next_index.get(node).unwrap();
                            let entries = self.raft_data.log.slice_from_index(ni as usize);
                            if !entries.is_empty() || HEARTBEAT_INTERVAL < elapsed_time {
                                log::info!(
                                    "{}",
                                    format!("Replicating {} and upwards to {}", ni, node)
                                );
                                replicated = true;
                                let append_payload = Payload::AppendEntries {
                                    term: self.get_current_term(),
                                    leader_id: self.node.clone(),
                                    prev_log_index: ni - 1,
                                    prev_log_term: self
                                        .raft_data
                                        .log
                                        .at(ni as usize - 1)
                                        .unwrap()
                                        .0,
                                    entries,
                                    leader_commit: self.raft_data.commit_index,
                                };

                                Message {
                                    src: self.node.clone(),
                                    dst: String::from(node),
                                    body: Body {
                                        id: None,
                                        in_reply_to: None,
                                        payload: append_payload,
                                    },
                                }
                                .send(&mut *output)
                                .with_context(|| format!("AppendEntries to {}", node))?;
                            }
                        }
                        if replicated {
                            self.raft_data.last_replication = Some(Instant::now());
                        }
                    }
                }
            },
            Event::EOF => {}
        }

        Ok(())
    }
}

impl KVNode {
    fn handle_txn(
        &mut self,
        txn: &Vec<(String, u32, Option<u32>)>,
        from: String,
        id: Option<usize>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<Option<Payload>> {
        if self.state() == State::Leader {
            let mut answer = Vec::with_capacity(txn.capacity());
            for cmd in txn {
                self.handle_command(cmd, &mut answer);
                let log_item = LogItem {
                    from: from.clone(),
                    msg_id: id.unwrap_or(0) as u32,
                    cmd: cmd.clone(),
                };
                self.raft_data
                    .log
                    .append(self.get_current_term(), Some(log_item));
            }
            Ok(Some(Payload::TxnOk { txn: answer }))
        } else if self.raft_data.presumed_leader.is_some() {
            // proxy to presumed leader
            Message {
                src: from.clone(),
                dst: self.raft_data.presumed_leader.as_ref().unwrap().clone(),
                body: Body {
                    id,
                    in_reply_to: None,
                    payload: Payload::Txn { txn: txn.clone() },
                },
            }
            .send(output)
            .with_context(|| format!("Proxy to failed"))?;
            Ok(None)
        } else {
            log::info!(
                "{}",
                format!(
                    "In leader election process; putting message in local queue: {:?}",
                    txn.clone()
                )
            );
            self.buffered
                .push_front((from.clone(), id.unwrap() as u32, txn.clone()));
            Ok(None)
        }
    }

    fn advance_map_from_log(&mut self) {
        while self.raft_data.last_applied < self.raft_data.commit_index {
            self.raft_data.last_applied += 1;
            let cmd = self
                .raft_data
                .log
                .at(self.raft_data.last_applied as usize)
                .unwrap();
            self.apply(&cmd.1.unwrap().cmd);
        }
    }

    fn write(&mut self, index: u32, to: u32) -> Option<u32> {
        self.store.insert(index, to)
    }

    fn read(&self, index: u32) -> Option<u32> {
        self.store.get(&index).map(|x| *x)
    }

    fn broadcast(&self, payload: Payload, output: &mut StdoutLock) -> anyhow::Result<()> {
        for node in &self.nodes {
            Message {
                src: self.node.clone(),
                dst: String::from(node),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: payload.clone(),
                },
            }
            .send(&mut *output)
            .with_context(|| format!("Broadcast to {}", node))?
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: &Command, answer: &mut Vec<Command>) {
        let mut add = |s, index, val| answer.push((s, index, val));
        let cmd = self.apply(cmd);
        add(cmd.0, cmd.1, cmd.2);
    }

    fn apply(&mut self, cmd: &Command) -> (String, u32, Option<u32>) {
        let mut res = (String::new(), 0, None);
        match cmd.0.as_str() {
            READ => res = (String::from(READ), cmd.1, self.read(cmd.1)),
            WRITE => {
                self.write(cmd.1, cmd.2.unwrap());
                res = (String::from(WRITE), cmd.1, cmd.2)
            }
            _ => {}
        }
        res
    }

    fn are_productive(&self) -> bool {
        self.state() == State::Leader || self.raft_data.presumed_leader.is_some()
    }

    fn set_payload_and_send(
        reply: &mut Message<Payload>,
        new_payload: Payload,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        reply.body.payload = new_payload;
        reply.send(output).context("sending broke")
    }
}

pub fn median<T: Ord + Copy>(mut c: Vec<T>) -> Option<T> {
    c.sort();
    c.get(c.len() - majority(c.len())).map(|n| *n)
}

pub fn majority(number_of_nodes: usize) -> usize {
    ((number_of_nodes as f64 / 2.0).floor() as usize) + 1
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KVNode, _, _>(())
}
