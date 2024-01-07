use anyhow::Context;
use maelstrom_convenience::{Node, Event, main_loop, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
    writer: std::sync::mpsc::Sender<Message<Payload>>,
}

impl Node<(), Payload> for EchoNode {
    fn from_init(
        _state: (),
        _init: maelstrom_convenience::Init,
        _inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, ()>>,
        message_writer: std::sync::mpsc::Sender<Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized {
        Ok(EchoNode { id: 1, writer: message_writer })
    }

    fn step(
        &mut self,
        input: maelstrom_convenience::Event<Payload, ()>
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let reply = input.into_reply(Some(&mut self.id));
        let p = match reply.body.payload {
            Payload::Echo { ref echo } => {
                Some(Payload::EchoOk { echo: echo.to_owned() })  
            },
            Payload::EchoOk { .. } => {None},
        };

        if let Some(payload) = p {
            self.set_payload_and_send(reply, payload)?
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
    main_loop::<_, EchoNode, _, _>(())
}