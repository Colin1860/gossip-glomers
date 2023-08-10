use std::{
    collections::{HashMap},
    io::StdoutLock,
    time::Duration,
};

use rand::Rng;

use anyhow::Context;
use maelstrom_convenience::{main_loop, Body, Event, Message, Node};
use serde::{Deserialize, Serialize};

const KEY: &str = "counter";
const SEQ_KV: &str = "seq-kv";
const MAGIC: usize = 1860;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Error {
        code: u64,
        text: String,
    },
    Add {
        delta: u64,
    },
    AddOk {},
    Read {
        key: Option<String>,
    },
    ReadOk {
        value: u64,
    },
    Cas {
        key: String,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
    },
    CasOk {},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {
    SendCas,
    Discover,
}

struct CounterNode {
    total_counter: u64,
    acc_delta: u64,
    id: usize,
    node_id: String,
    neighbors: HashMap<String, u64>,
    sync: std::sync::mpsc::Sender<()>,
}

impl CounterNode {
    #[inline(always)]
    fn set_payload_and_send(
        reply: &mut Message<Payload>,
        new_payload: Payload,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        reply.body.payload = new_payload;
        reply.send(output).context("sending broke")
    }

    #[inline(always)]
    fn set_payload_and_send_to_kv(
        reply: &mut Message<Payload>,
        new_payload: Payload,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        reply.body.payload = new_payload;
        reply.dst = SEQ_KV.into();
        reply.send(output).context("sending broke")
    }

    fn calc(&mut self) -> u64 {
        let mut counter = self.acc_delta;
        for (_k, v) in &self.neighbors {
            counter += *v;
        }
        counter
    }
}

impl Node<(), Payload, InjectedPayload> for CounterNode {
    fn from_init(
        _state: (),
        init: maelstrom_convenience::Init,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            // give each node a slightly different delay
            let mut rng = rand::thread_rng();
            let num = rng.gen_range(0..3) as u64;
            std::thread::sleep(Duration::from_millis(100*num));
            loop {
                // This should read on EOF
                if let Ok(_) = rx.try_recv() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(800));
                if let Err(_) = inject.send(Event::Injected(InjectedPayload::Discover)) {
                    break;
                }

                std::thread::sleep(Duration::from_millis(200));

                if let Err(_) = inject.send(Event::Injected(InjectedPayload::SendCas)) {
                    break;
                }
            }
        });

        Ok(CounterNode {
            id: 1,
            neighbors: init
                .node_ids
                .into_iter()
                .filter(|id| *id != init.node_id)
                .map(|id| (id, 0 as u64))
                .collect(),
            node_id: init.node_id,
            total_counter: 0,
            acc_delta: 0,
            sync: tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        eprintln!("Reading input");
        match input {
            Event::Message(message) => {
                let id = message.body.id;
                let from = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                let mut payload = None;
                match reply.body.payload {
                    Payload::Error { code, .. } => {
                        match code {
                            // precondition failed error, see here: https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
                            22 => {
                                let read_payload = Payload::Read {
                                    key: Some(KEY.into()),
                                };
                                CounterNode::set_payload_and_send_to_kv(
                                    &mut reply,
                                    read_payload,
                                    output,
                                )?;
                            }
                            _ => {
                                panic!("Unhandled error");
                            }
                        }
                    }
                    Payload::Add { delta } => {
                        self.acc_delta += delta;
                        payload = Some(Payload::AddOk {})
                    }
                    Payload::AddOk {} => {}
                    Payload::CasOk {} => {
                        self.total_counter = self.calc();
                    }
                    Payload::Read { .. } => {
                        let which_counter = if let Some(MAGIC) = id {
                            self.acc_delta
                        } else {
                            self.total_counter
                        };
                        payload = Some(Payload::ReadOk {
                            value: which_counter,
                        })
                    }
                    Payload::ReadOk { value } => {
                        if from == SEQ_KV {
                            self.total_counter = value;
                        } else {
                            *self.neighbors.entry(from.clone()).or_insert(0) = value;
                        }
                    }
                    // the rest are messages to the seq-kv
                    _ => {}
                }

                if payload.is_some() {
                    CounterNode::set_payload_and_send(&mut reply, payload.unwrap(), output)?
                }
                Ok(())
            }
            Event::Injected(message) => {
                match message {
                    InjectedPayload::SendCas => {
                        let new_counter = self.calc();
                        let cas_payload = Payload::Cas {
                            key: KEY.into(),
                            from: self.total_counter,
                            to: new_counter,
                            create_if_not_exists: true,
                        };

                        let m = Message {
                            src: self.node_id.clone(),
                            dst: SEQ_KV.into(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: cas_payload,
                            },
                        };
                        m.send(output)?
                    }
                    InjectedPayload::Discover => {
                        for (key, _) in &self.neighbors {
                            let read_payload = Payload::Read { key: None };
                            let m = Message {
                                src: self.node_id.clone(),
                                dst: key.clone(),
                                body: Body {
                                    id: Some(MAGIC),
                                    in_reply_to: None,
                                    payload: read_payload,
                                },
                            };

                            m.send(output)?
                        }
                    }
                }
                Ok(())
            }
            Event::EOF => self.sync.send(()).context("syncing fail"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, CounterNode, _, _>(())
}
