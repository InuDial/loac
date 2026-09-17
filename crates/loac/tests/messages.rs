#[path = "messages/admission.rs"]
mod admission;
#[path = "messages/capacity_wakers.rs"]
mod capacity_wakers;
#[path = "messages/policies.rs"]
mod policies;
#[path = "messages/stream_reply.rs"]
mod stream_reply;
#[path = "messages/stream_to.rs"]
mod stream_to;
mod support;
#[path = "messages/typed_replies.rs"]
mod typed_replies;

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use loac::{
    Actor, ActorScope, Cx, DispatchHandler, Handler, Message, ReplyExt, SpawnOptions, actor,
};
use tokio::sync::oneshot;

use support::lock;

struct SerialActor {
    committed: Arc<Mutex<Vec<u8>>>,
}

#[actor(mailbox = dynamic, interleaved)]
impl Actor for SerialActor {
    type SpawnArgs = Arc<Mutex<Vec<u8>>>;

    async fn init(committed: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self { committed }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Block {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<Block> for SerialActor {
    async fn handle(message: Block, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct Record(u8);

impl DispatchHandler<Record> for SerialActor {
    fn handle(
        &mut self,
        message: Record,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, Record> {
        lock(&self.committed).push(message.0);
        message.0.ready()
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Notify(u8);

impl DispatchHandler<Notify> for SerialActor {
    fn handle(
        &mut self,
        message: Notify,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, Notify> {
        lock(&self.committed).push(message.0);
        ().ready()
    }
}

fn single_slot_options() -> SpawnOptions<SerialActor> {
    let one = NonZeroUsize::new(1).expect("one is non-zero");
    SpawnOptions::<SerialActor>::default().with_mailbox_capacity(one)
}
