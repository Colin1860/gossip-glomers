use anyhow::Context;
use maelstrom_convenience::{Node, Event, main_loop};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize
}

impl Node<(), Payload> for EchoNode {
    fn from_init(
        _state: (),
        _init: maelstrom_convenience::Init,
        _inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized {
        Ok(EchoNode { id: 1 })
    }

    fn step(
        &mut self,
        input: maelstrom_convenience::Event<Payload, ()>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                serde_json::to_writer(&mut *output, &reply).context("serialize response init crash")?;
                output.write_all(b"\n").context("write trailing new line")?;
            },
            Payload::EchoOk { .. } => {},
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _, _>(())
}