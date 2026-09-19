use loac::{Actor, ActorScope, Cx, ExitReason, Handler, Message, Shutdown, actor};
use tokio::sync::mpsc;

use super::support::watchdog;

struct Calculator(u64);

#[actor(mailbox)]
impl Actor for Calculator {
    type SpawnArgs = u64;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Add(u64);

impl Handler<Add> for Calculator {
    async fn handle(message: Add, mut cx: Cx<'_, Self>) -> u64 {
        cx.with(|actor, _| {
            actor.0 += message.0;
            actor.0
        })
    }
}

#[derive(Message)]
#[message(reply = String)]
struct Describe;

impl Handler<Describe> for Calculator {
    async fn handle(_message: Describe, mut cx: Cx<'_, Self>) -> String {
        cx.with(|actor, _| format!("count={}", actor.0))
    }
}

#[tokio::test]
async fn one_actor_handles_multiple_typed_message_replies() {
    let owner = loac::spawn::<Calculator>(0);
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(Add(3))).await.unwrap(), 3);
    assert_eq!(watchdog(actor.call(Add(4))).await.unwrap(), 7);
    assert_eq!(watchdog(actor.call(Describe)).await.unwrap(), "count=7");
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Drain)).await.reason(),
        ExitReason::Drained
    );
}

#[derive(Message)]
#[message(reply = mpsc::Receiver<u8>)]
struct Events;

impl Handler<Events> for Calculator {
    async fn handle(_message: Events, _cx: Cx<'_, Self>) -> mpsc::Receiver<u8> {
        let (events, receiver) = mpsc::channel(2);
        events.try_send(1).expect("the stream buffer has room");
        events.try_send(2).expect("the stream buffer has room");
        receiver
    }
}

#[tokio::test]
async fn a_stream_handle_is_an_ordinary_typed_reply() {
    let owner = loac::spawn::<Calculator>(0);
    let actor = owner.actor_ref();
    let mut events = watchdog(actor.call(Events)).await.unwrap();

    assert_eq!(watchdog(events.recv()).await, Some(1));
    assert_eq!(watchdog(events.recv()).await, Some(2));
    assert_eq!(watchdog(events.recv()).await, None);
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Drain)).await.reason(),
        ExitReason::Drained
    );
}
