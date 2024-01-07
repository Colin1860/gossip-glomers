use anyhow::Context;
use maelstrom_convenience::{main_loop, Event, Node, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate {},
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueIDNode {
    id: usize,
    node: String,
    writer: std::sync::mpsc::Sender<Message<Payload>>
}

impl Node<(), Payload> for UniqueIDNode {
    fn from_init(
        _state: (),
        init: maelstrom_convenience::Init,
        _inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, ()>>,
        message_writer: std::sync::mpsc::Sender<Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIDNode {
            id: 1,
            node: init.node_id,
            writer: message_writer
        })
    }

    fn step(
        &mut self,
        input: maelstrom_convenience::Event<Payload, ()>,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate {} => {
                let guid = format!("{}-{}", self.node, self.id);
                self.set_payload_and_send(reply, Payload::GenerateOk { guid })?;
            }
            Payload::GenerateOk { .. } => {}
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

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueIDNode, _, _>(())
}
