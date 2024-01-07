use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};


#[derive(PartialEq, Clone, Copy)]
pub enum State {
    Candidate,
    Follower,
    Leader,
}

// Time before next election, in seconds
pub const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);
// Time between heartbeats in seconds
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
// Dont replicate to frequently
pub const MIN_REPLICATION_INTERVAL: Duration = Duration::from_millis(50);

pub struct RaftData<T: Clone> {
    // Timeouts and deadlines
    pub election_deadline: Instant,
    pub stepdown_deadline: Instant,
    // None if we want to trigger instant replication
    pub last_replication: Option<Instant>,

    // Components
    pub log: Log<T>,

    // Raft state
    pub state: State,
    pub term: u32,
    pub voted_for: Option<String>,
    pub presumed_leader: Option<String>,

    // Leader state
    pub last_applied: u32,
    pub commit_index: u32,
    pub next_index: HashMap<String, u32>,
    pub match_index: HashMap<String, u32>,

    // Convenience
    pub votes: HashSet<String>,
    pub term_vote_started: u32,
}

pub trait Stateful {
    fn state(&self) -> State;

    fn set_state(&mut self, state: State);
}

pub trait Raft: Stateful {
    type LogItem;
    type Packet;

    // Handlers
    fn request_votes(&mut self);

    fn handle_vote(&mut self, from: String, term: u32, granted: bool);

    fn handle_vote_request(
        &mut self,
        remote_term: u32,
        candidate: String,
        last_log_index: u32,
        last_log_term: u32,
    ) -> (u32, bool);

    fn handle_append(
        &mut self,
        term: u32,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: &Vec<(u32, Option<Self::LogItem>)>,
        leader_commit: u32,
    ) -> Self::Packet;

    // Helpers
    fn advance_commit_index(&mut self) -> Vec<(String, u32, Self::Packet)>;

    fn match_index(&mut self);

    fn get_current_term(&self) -> u32;

    fn advance_term(&mut self, term: u32);

    fn update_indices(&mut self);

    fn vote_for(&mut self, vote: String);

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

    // State transitions
    fn become_candidate(&mut self, name: String) {
        self.set_state(State::Candidate);
        self.advance_term(self.get_current_term() + 1);
        self.vote_for(name);
        self.reset_presumed_leader();
        self.reset_election_deadline();
        self.reset_stepdown_deadline();
        self.request_votes();
        log::info!(
            "{}",
            format!("Became candidate for term: {}", self.get_current_term())
        );
    }

    fn become_leader(&mut self) {
        assert!(self.state() == State::Candidate);
        self.set_state(State::Leader);
        self.reset_presumed_leader();
        self.reset_last_replication();
        self.update_indices();
        self.reset_stepdown_deadline();
        log::info!(
            "{}",
            format!("Became leader for term: {}", self.get_current_term())
        );
    }

    fn become_follower(&mut self) {
        self.set_state(State::Follower);
        self.reset_match_index();
        self.reset_next_index();
        self.reset_presumed_leader();
        self.reset_election_deadline();
        log::info!(
            "{}",
            format!("Became follower for term: {}", self.get_current_term())
        );
    }

    // resets
    fn reset_presumed_leader(&mut self);

    fn reset_match_index(&mut self);

    fn reset_next_index(&mut self);

    fn reset_last_replication(&mut self);

    fn reset_election_deadline(&mut self);

    fn reset_stepdown_deadline(&mut self);
}

#[derive(Debug)]
pub struct Log<T: Clone> {
    entries: Vec<(u32, Option<T>)>,
}

impl<T: Clone> Log<T> {
    pub fn new() -> Self {
        Self {
            entries: vec![(0, None)],
        }
    }

    pub fn truncate(&mut self, len: usize) { 
        self.entries.truncate(len - 1);
    }

    pub fn at(&self, index: usize) -> Option<(u32, Option<T>)> {
        self.entries.get(index - 1).map(|i| i.clone())
    }

    pub fn append(&mut self, term: u32, item: Option<T>) {
        self.entries.push((term, item))
    }

    pub fn append_all(&mut self, mut other: Vec<(u32, Option<T>)>) {
        self.entries.append(&mut other)
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    pub fn slice_from_index(&mut self, index: usize) -> Vec<(u32, Option<T>)> {
        // 2 because we want to skip one before the index and raft is indexed by one
        self.entries.iter().skip(index - 2).cloned().collect()
    }

    pub fn last(&self) -> (u32, Option<T>) {
        self.entries[self.entries.len() - 1].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate() {
        // Arrange
        let mut log: Log<u32> = Log {
            entries: vec![
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, None),
                (7, None),
                (8, None),
                (9, None),
                (10, None),
            ],
        };

        // Act
        log.truncate(7);

        // Assert
        assert_eq!(
            log.entries,
            vec![
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, None)
            ]
        );
    }
}
