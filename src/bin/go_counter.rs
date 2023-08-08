use std::{io::StdoutLock, time::Duration};

use anyhow::Context;
use maelstrom_convenience::{main_loop, Body, Event, Message, Node};
use serde::{Deserialize, Serialize};

const KEY: &str = "counter";
const SEQ_KV: &str = "seq-kv";

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
    SendRead,
}

struct CounterNode {
    counter: u64,
    tmp_counter: u64,
    id: usize,
    node_id: String,
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
            // generate gossip events
            let mut loop_count = 0;
            loop {
                // This should read on EOF
                if let Ok(_) = rx.try_recv() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
                loop_count += 1;
                if let Err(_) = inject.send(Event::Injected(InjectedPayload::SendCas)) {
                    break;
                }

                if loop_count == 5 {
                    loop_count = 0;
                    if let Err(_) = inject.send(Event::Injected(InjectedPayload::SendRead)) {
                        break;
                    }
                }
            }
        });

        Ok(CounterNode {
            id: 1,
            node_id: init.node_id,
            counter: 0,
            tmp_counter: 0,
            sync: tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(message) => {
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
                        self.tmp_counter += delta;
                        payload = Some(Payload::AddOk {})
                    }
                    Payload::AddOk {} => {}
                    Payload::Read { .. } => {
                        payload = Some(Payload::ReadOk {
                            value: self.counter,
                        })
                    }
                    Payload::ReadOk { value } => self.counter = value,
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
                        if self.counter != self.tmp_counter {
                            let cas_payload = Payload::Cas {
                                key: KEY.into(),
                                from: self.counter,
                                to: self.tmp_counter,
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
                    }
                    InjectedPayload::SendRead => {
                        let read_payload = Payload::Read {
                            key: Some(KEY.into()),
                        };
                        let m = Message {
                            src: self.node_id.clone(),
                            dst: SEQ_KV.into(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: read_payload,
                            },
                        };
                        m.send(output)?
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
