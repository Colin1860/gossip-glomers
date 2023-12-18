use std::{collections::HashSet, time::{Duration, Instant}};

#[derive(PartialEq, Clone, Copy)]
pub enum State {
    Candidate,
    Follower,
    Leader,
}

pub struct RaftData {
    pub votes: HashSet<String>,
    pub voted_for: Option<String>,
    pub election_timeout: Duration,
    pub election_deadline: Instant,
    pub stepdown_deadline: Instant,
    pub log: Log,
    pub term_vote_started: u32,
    pub term: u32,
    pub state: State,
}

pub trait Stateful {
    fn get_state(&self) -> State;

    fn set_state(&mut self, state: State);
}

pub trait Raft: Stateful {
    fn advance_term(&mut self, term: u32);

    fn get_current_term(&self) -> u32;

    fn request_votes(&mut self);

    fn handle_vote(&mut self, from: String, term: u32, granted: bool);

    fn handle_vote_request(&mut self,
        remote_term: u32,
        candidate: String,
        last_log_index: u32,
        last_log_term: u32,
    ) -> (u32, bool);

    fn vote_for(&mut self, vote: String);

    fn majority(number_of_nodes: usize) -> usize {
        ((number_of_nodes as f64 / 2.0).floor() as usize) + 1
    }

    fn become_candidate(&mut self, name: String) {
        self.set_state(State::Candidate);
        self.advance_term(self.get_current_term() + 1);
        self.vote_for(name);
        self.reset_election_deadline();
        self.reset_stepdown_deadline();
        log::info!(
            "{}",
            format!("Became candidate for term: {}", self.get_current_term())
        );
        self.request_votes();
    }

    fn become_leader(&mut self) {
        assert!(self.get_state() == State::Candidate);
        self.set_state(State::Leader);
        self.reset_stepdown_deadline();
        log::info!(
            "{}",
            format!("Became leader for term: {}", self.get_current_term())
        );
    }

    fn become_follower(&mut self) {
        self.set_state(State::Follower);
        log::info!(
            "{}",
            format!("Became follower for term: {}", self.get_current_term())
        );
    }

    fn maybe_step_down(&mut self, remote_term: u32) {
        if self.get_current_term() < remote_term {
            log::info!(
                "{}",
                format!(
                    "Stepping down: remote term {} higher than our term {}",
                    remote_term,
                    self.get_current_term()
                )
            );
            self.advance_term(remote_term);
            self.become_follower();
        }
    }

    fn reset_election_deadline(&mut self);

    fn reset_stepdown_deadline(&mut self);
}

pub struct Log {
    entries: Vec<u32>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![0],
        }
    }

    pub fn at(&self, index: usize) -> u32 {
        self.entries[index - 1]
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    pub fn last(&self) -> u32 {
        self.entries[self.entries.len() - 1]
    }
}
