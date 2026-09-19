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

use loac::{Actor, ActorScope, Cx, Handler, Message, SpawnOptions, actor};
use tokio::sync::oneshot;

use support::lock;

struct MailboxActor {
    committed: Arc<Mutex<Vec<u8>>>,
}

#[actor(mailbox, mailbox_capacity = dynamic)]
impl Actor for MailboxActor {
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

impl Handler<Block> for MailboxActor {
    async fn handle(message: Block, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct Record(u8);

impl Handler<Record> for MailboxActor {
    async fn handle(message: Record, mut cx: Cx<'_, Self>) -> u8 {
        cx.with(|actor, _| lock(&actor.committed).push(message.0));
        message.0
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Notify(u8);

impl Handler<Notify> for MailboxActor {
    async fn handle(message: Notify, mut cx: Cx<'_, Self>) {
        cx.with(|actor, _| lock(&actor.committed).push(message.0));
    }
}

fn single_slot_options() -> SpawnOptions<MailboxActor> {
    let one = NonZeroUsize::new(1).expect("one is non-zero");
    SpawnOptions::<MailboxActor>::default().with_mailbox_capacity(one)
}
