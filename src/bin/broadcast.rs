use anyhow::Context;
use maelstrom_convenience::{main_loop, Body, Event, Message, Node};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Gossip {
        seen: HashSet<usize>,
    },
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
}

/// Injected payloads are payloads that we send to ourself and have to handle
/// separately
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    id: usize,
    node: String,
    messages: HashSet<usize>,
    neighborhood: HashSet<String>,
    estimate: HashMap<String, HashSet<usize>>,
    gossip_sync: std::sync::mpsc::Sender<()>,
    writer: std::sync::mpsc::Sender<Message<Payload>>
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: maelstrom_convenience::Init,
        inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, InjectedPayload>>,
        message_writer: std::sync::mpsc::Sender<Message<Payload>>
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            // generate gossip events
            loop {
                // This should read on EOF
                if let Ok(_) = rx.try_recv() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
                if let Err(_) = inject.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(BroadcastNode {
            id: 1,
            node: init.node_id,
            messages: HashSet::default(),
            neighborhood: HashSet::default(),
            estimate: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            gossip_sync: tx,
            writer: message_writer,
        })
    }

    fn step(
        &mut self,
        input: maelstrom_convenience::Event<Payload, InjectedPayload>,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(message) => {
                let mut reply = message.into_reply(Some(&mut self.id));
                let mut payload = None;
                match reply.body.payload {
                    Payload::Gossip { ref seen } => self.handle_gossip(&seen, &reply.dst),
                    Payload::Broadcast { message } => {
                        payload = Some(self.handle_broadcast(message))
                    }
                    Payload::Read => payload = Some(self.handle_read()),
                    Payload::Topology { ref mut topology } => {
                        payload = Some(self.handle_topology(topology))
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => (),
                };
                if payload.is_some() {
                    self.set_payload_and_send(reply, payload.unwrap())?
                }
            }
            Event::Injected(injected) => match injected {
                InjectedPayload::Gossip => {
                    // let mut rng = rand::thread_rng();
                    // let selected_neighbors = &self
                    //     .neighborhood
                    //     .iter()
                    //     .choose_multiple(&mut rng, &self.neighborhood.len() / 2);
                    let selected_neighbors = &self.neighborhood;

                    for n in selected_neighbors {
                        let known_to_n = &self.estimate[n];
                        let notify_of: HashSet<_> = self
                            .messages
                            .iter()
                            .copied()
                            .filter(|x| !known_to_n.contains(x))
                            .collect();

                        self.writer.send(Message {
                            src: self.node.clone(),
                            dst: String::from(n),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        })?;
                    }
                }
            },
            Event::EOF => self.gossip_sync.send(())?,
        }

        Ok(())
    }

    #[inline(always)]
    fn set_payload_and_send(&self,
        mut reply: Message<Payload>,
        new_payload: Payload,
    ) -> anyhow::Result<()> {
        reply.body.payload = new_payload;
        self.writer.send(reply.clone()).context("sending to writer thread err'd")
    }
}

impl BroadcastNode {
    fn handle_gossip(&mut self, seen: &HashSet<usize>, from: &String) {
        self.estimate
            .get_mut(from)
            .expect("got message from unknown")
            .extend(seen.iter().copied());
        self.messages.extend(seen);
    }

    fn handle_broadcast(&mut self, message: usize) -> Payload {
        self.messages.insert(message);
        Payload::BroadcastOk
    }

    fn handle_read(&self) -> Payload {
        Payload::ReadOk {
            messages: self.messages.clone(),
        }
    }

    fn handle_topology(&mut self, topology: &mut HashMap<String, HashSet<String>>) -> Payload {
        if let Some(neighborhood) = topology.remove(&self.node) {
            self.neighborhood = neighborhood;
        } else {
            // go in to error state, means that a node send a faulty topology
        };
        Payload::TopologyOk
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
