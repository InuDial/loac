mod support;

use loac::{Actor, ActorScope, Cx, ExitReason, Handler, Message, Shutdown, actor};

use support::watchdog;

#[derive(Message)]
#[message(reply = u8)]
struct Read;

struct FirstActor(u8);

#[actor(mailbox)]
impl Actor for FirstActor {
    type SpawnArgs = u8;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

struct SecondActor(u8);

#[actor(mailbox)]
impl Actor for SecondActor {
    type SpawnArgs = u8;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

impl Handler<Read> for FirstActor {
    async fn handle(_message: Read, mut cx: Cx<'_, Self>) -> u8 {
        cx.with(|actor, _| actor.0)
    }
}

impl Handler<Read> for SecondActor {
    async fn handle(_message: Read, mut cx: Cx<'_, Self>) -> u8 {
        cx.with(|actor, _| actor.0)
    }
}

#[tokio::test]
async fn handler_context_selects_each_actor_type() {
    // Both handlers use one message type.
    // Each Cx still resolves its actor type.
    let first_owner = loac::spawn::<FirstActor>(1);
    let second_owner = loac::spawn::<SecondActor>(2);
    let first = first_owner.actor_ref();
    let second = second_owner.actor_ref();

    let (first_reply, second_reply) =
        tokio::join!(watchdog(first.call(Read)), watchdog(second.call(Read)),);
    assert_eq!(first_reply, Ok(1));
    assert_eq!(second_reply, Ok(2));

    let (first_exit, second_exit) = tokio::join!(
        watchdog(first_owner.shutdown(Shutdown::Stop)),
        watchdog(second_owner.shutdown(Shutdown::Stop)),
    );
    assert_eq!(first_exit.reason(), ExitReason::Stopped);
    assert_eq!(second_exit.reason(), ExitReason::Stopped);
}
