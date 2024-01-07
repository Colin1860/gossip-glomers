use std::collections::HashMap;

use anyhow::Context;
use maelstrom_convenience::{main_loop, Event, Message, Node};
use serde::{Deserialize, Serialize};

pub struct Storage {
    logs: HashMap<String, Log>,
}

pub struct Log {
    commited_upto: usize,
    items: Vec<LogItem>,
}

pub struct LogItem {
    value: usize,
    offset: usize,
    commited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KVCommunication {
    Error {
        code: u64,
        text: String,
    },
    Read {
        key: String,
    },
    ReadOk {
        value: u64,
    },
    Write {
        key: String,
        value: usize
    },
    WriteOk {

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
enum Payload {
    Error {
        code: u64,
        text: String,
    },
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[usize; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InjectedPayload {}

pub struct KafkaNode {
    id: usize,
    storage: Storage,
    sync: std::sync::mpsc::Sender<()>,
    writer: std::sync::mpsc::Sender<Message<Payload>>,
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _state: (),
        _init: maelstrom_convenience::Init,
        _inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
        message_writer: std::sync::mpsc::Sender<Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (tx, _rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            // give each node a slightly different delay
        });

        Ok(KafkaNode {
            id: 1,
            storage: Storage {
                logs: HashMap::new(),
            },
            sync: tx,
            writer: message_writer,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(message) => {
                let reply = message.into_reply(Some(&mut self.id));
                let mut payload = None;
                match reply.body.payload {
                    Payload::Error { .. } => todo!(),
                    Payload::Send { ref key, msg } => {
                        let log = self.storage.logs.entry(key.clone()).or_insert(Log {
                            commited_upto: 0,
                            items: Vec::default(),
                        });
                        let offset = log.items.len();
                        log.items.push(LogItem {
                            value: msg,
                            offset,
                            commited: false,
                        });
                        payload = Some(Payload::SendOk { offset });
                    }
                    Payload::SendOk { .. } => {}
                    Payload::Poll { ref offsets } => {
                        let mut poll_result: HashMap<String, Option<Vec<[usize; 2]>>> = offsets
                            .clone()
                            .into_iter()
                            .map(|(key, _)| (key, None))
                            .collect();
                        for (wanted_log, wanted_offset) in offsets {
                            let log = self.storage.logs.get_mut(wanted_log);
                            let Some(log) = log else{
                                continue;
                            };

                            let slice: Vec<[usize; 2]> = if *wanted_offset < log.items.len() {
                                log.items[*wanted_offset..]
                                    .iter()
                                    .clone()
                                    .map(|l| [l.offset, l.value])
                                    .collect()
                            } else {
                                Vec::new()
                            };

                            poll_result.insert(wanted_log.clone(), Some(slice));
                        }
                        let poll_result = poll_result
                            .into_iter()
                            .map(|(k, v)| (k, v.unwrap_or(Vec::new())))
                            .collect();
                        payload = Some(Payload::PollOk { msgs: poll_result })
                    }
                    Payload::PollOk { .. } => {}
                    Payload::CommitOffsets { ref offsets } => {
                        for (k, upto_offset) in offsets {
                            let log = self.storage.logs.get_mut(k);
                            let Some(log) = log else{
                                continue;
                            };
                            log.items[log.commited_upto..]
                                .iter_mut()
                                .take_while(|log_item| log_item.offset <= *upto_offset)
                                .for_each(|log_item| log_item.commited = true);
                            log.commited_upto = std::cmp::min(*upto_offset, log.items.len())
                        }
                        payload = Some(Payload::CommitOffsetsOk {})
                    }
                    Payload::CommitOffsetsOk {} => {}
                    Payload::ListCommittedOffsets { ref keys } => {
                        let mut res: HashMap<String, usize> =
                            keys.clone().into_iter().map(|key| (key, 0)).collect();
                        for key in keys {
                            if let Some(log) = self.storage.logs.get(key) {
                                res.insert(key.clone(), log.commited_upto);
                            }
                        }
                        payload = Some(Payload::ListCommittedOffsetsOk { offsets: res })
                    }
                    Payload::ListCommittedOffsetsOk { .. } => {}
                }
                if payload.is_some() {
                    self.set_payload_and_send(reply, payload.unwrap())?
                }
                Ok(())
            }
            Event::Injected(message) => match message {},
            Event::EOF => self.sync.send(()).context("syncing fail"),
        }
    }

    fn set_payload_and_send(&self,
        mut reply: Message<Payload>,
        new_payload: Payload,
    ) -> anyhow::Result<()> {
        reply.body.payload = new_payload;
        self.writer.send(reply.clone()).context("sending to writer thread err'd")
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaNode, _, _>(())
}
