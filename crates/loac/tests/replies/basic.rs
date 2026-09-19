use super::*;

struct Counter(u8);

#[actor(mailbox)]
impl Actor for Counter {
    type SpawnArgs = u8;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct Increment;

impl Handler<Increment> for Counter {
    async fn handle(_message: Increment, mut cx: Cx<'_, Self>) -> u8 {
        cx.with(|actor, _| {
            actor.0 += 1;
            actor.0
        })
    }
}

#[tokio::test]
async fn handler_mutates_actor_and_replies() {
    let owner = loac::spawn::<Counter>(0);
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(Increment)).await, Ok(1));
    assert_eq!(watchdog(actor.call(Increment)).await, Ok(2));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

#[derive(Message)]
#[message(reply = u8)]
struct ConstructReply;

impl Handler<ConstructReply> for Counter {
    fn handle(
        _message: ConstructReply,
        mut cx: Cx<'_, Self>,
    ) -> impl Future<Output = u8> + Send + '_ {
        let value = cx.with(|actor, _| {
            actor.0 += 1;
            actor.0
        });
        std::future::ready(value)
    }
}

#[tokio::test]
async fn handler_construction_can_access_actor_state() {
    let owner = loac::spawn::<Counter>(0);

    assert_eq!(watchdog(owner.call(ConstructReply)).await, Ok(1));
    assert_eq!(watchdog(owner.call(Increment)).await, Ok(2));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

#[derive(Message)]
#[message(reply = u8)]
struct ChooseReply(bool);

impl Handler<ChooseReply> for Counter {
    async fn handle(message: ChooseReply, _cx: Cx<'_, Self>) -> u8 {
        if message.0 { 1 } else { 2 }
    }
}

#[tokio::test]
async fn handler_branches_without_reply_wrappers() {
    let owner = loac::spawn::<Counter>(0);
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(ChooseReply(true))).await, Ok(1));
    assert_eq!(watchdog(actor.call(ChooseReply(false))).await, Ok(2));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}
