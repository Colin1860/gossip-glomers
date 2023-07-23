use maelstrom_convenience::{main_loop, Event, Node};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock};

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
}

impl Node<(), Payload> for UniqueIDNode {
    fn from_init(
        _state: (),
        init: maelstrom_convenience::Init,
        _inject: std::sync::mpsc::Sender<maelstrom_convenience::Event<Payload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIDNode {
            id: 1,
            node: init.node_id,
        })
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
            Payload::Generate {} => {
                let guid = format!("{}-{}", self.node, self.id);
                reply.body.payload = Payload::GenerateOk { guid };
                reply.send(&mut *output)?;
            }
            Payload::GenerateOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueIDNode, _, _>(())
}
