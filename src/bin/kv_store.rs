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
    raft::{Log, Raft, RaftData, State, Stateful},
    Body, Event, Message, Node,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    thread,
    time::{Duration, Instant},
};

const READ: &str = "r";
const WRITE: &str = "w";
const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

pub type Command = (String, u32, Option<u32>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
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
}

enum InternalCommunication {
    RunCandidate,
    StepDown,
}

/// Injected payloads are payloads that we send to ourself and have to handle
/// separately
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    Req,
}

struct KVNode {
    id: usize,
    node: String,
    nodes: HashSet<String>,
    store: HashMap<u32, u32>,
    raft_data: RaftData,
    injector: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, InjectedPayload>>,
    comm: crossbeam::channel::Receiver<InternalCommunication>,
}

impl Stateful for KVNode {
    // #[inline(always)]
    fn get_state(&self) -> State {
        self.raft_data.state
    }

    // #[inline(always)]
    fn set_state(&mut self, state: State) {
        self.raft_data.state = state
    }
}

impl Raft for KVNode {
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
            rng.gen_range(0.0..self.raft_data.election_timeout.as_secs_f32()),
        ));
        if let Some(new_timeline) = new_timeline {
            self.raft_data.election_deadline = new_timeline;
        }
    }

    fn reset_stepdown_deadline(&mut self) {
        self.raft_data.stepdown_deadline = Instant::now() + self.raft_data.election_timeout;
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
                    "Candidate term {} lower than {}, not gran
                    ting vote.",
                    remote_term, our_term
                )
            );
        } else if let Some(vote) = self.raft_data.voted_for.as_ref() {
            log::info!(
                "{}",
                format!("Already voted for {}; not granting vote.", vote)
            );
        } else if last_log_term < self.raft_data.log.last() {
            log::info!(
                "{}",
                format!(
                    "Have log entries from term {}, which is ne
                    wer than remote term {}; not granting vote.",
                    self.raft_data.log.last(),
                    last_log_term
                )
            );
        } else if last_log_term == self.raft_data.log.last()
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
            if self.raft_data.votes.len() >= Self::majority(self.nodes.len()) {
                self.become_leader();
            }
        }
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
        let logfile = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
            .build("/Users/colinmikolajczak/Code/flyio/flyio-challenge/log/output.log")?;

        let config = Config::builder()
            .appender(Appender::builder().build("logfile", Box::new(logfile)))
            .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

        log4rs::init_config(config)?;

        let (s, r) = unbounded();
        let s1 = s.clone();
        thread::Builder::new().name("RunCandidateThread".to_string()).spawn(move || loop {
            thread::sleep(Duration::from_secs_f32(0.1));
            let rand_sleep = rand::thread_rng().gen_range(0.0..1.0) / 10.0;
            thread::sleep(Duration::from_secs_f64(rand_sleep));
            let _ = s1.send(InternalCommunication::RunCandidate);
        })?;

        thread::Builder::new().name("StepDownThread".to_string()).spawn(move || loop {
            thread::sleep(Duration::from_secs_f32(0.1));
            let _ = s.send(InternalCommunication::StepDown);
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
                voted_for: None,
                term: 0,
                term_vote_started: 0,
                state: State::Follower,
                votes: HashSet::new(),
                log: Log::new(),
                stepdown_deadline: Instant::now(),
                election_deadline: Instant::now(),
                election_timeout: ELECTION_TIMEOUT,
            },
            store: HashMap::default(),
            comm: r,
            injector: inject.clone(),
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
                    if self.get_state() != State::Leader {
                        self.become_candidate(self.node.clone());
                    } else {
                        self.reset_election_deadline();
                    }
                }
            }
            Ok(InternalCommunication::StepDown) => {
                if self.raft_data.state == State::Leader && self.raft_data.stepdown_deadline < Instant::now() {
                    log::info!("Stepping down: haven't received any acks lately");
                    self.become_follower();
                }
            }
            _ => {}
        }

        match input {
            Event::Message(message) => {
                let id = message.body.id;
                let from = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                reply.body.in_reply_to = id;
                let new_payload = match reply.body.payload {
                    Payload::Txn { ref txn } => {
                        let mut answer = Vec::with_capacity(txn.capacity());
                        for cmd in txn {
                            self.handle_command(cmd, &mut answer);
                        }
                        Some(Payload::TxnOk { txn: answer })
                    }
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
                        last_log_term: self.raft_data.log.last(),
                    };
                    self.raft_data.term_vote_started = term;
                    self.broadcast(payload, output)?
                }
            },
            Event::EOF => {}
        }

        Ok(())
    }
}

impl KVNode {
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
        match cmd.0.as_str() {
            READ => {
                add(String::from(READ), cmd.1, self.read(cmd.1));
            }
            WRITE => {
                self.write(cmd.1, cmd.2.unwrap());
                add(String::from(WRITE), cmd.1, cmd.2)
            }
            _ => {}
        }
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

fn main() -> anyhow::Result<()> {
    main_loop::<_, KVNode, _, _>(())
}
