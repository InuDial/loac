use super::*;
use loac::Writer;

struct CxExclusiveCounter(u8);

#[actor(mailbox)]
impl Actor for CxExclusiveCounter {
    type SpawnArgs = u8;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct CxExclusiveIncrement;

impl Handler<CxExclusiveIncrement> for CxExclusiveCounter {
    async fn handle(_message: CxExclusiveIncrement, mut cx: Cx<'_, Self>) -> u8 {
        let mut guard = cx.exclusive();
        guard.with(|actor, _| {
            actor.0 += 1;
            actor.0
        })
    }
}

#[tokio::test]
async fn cx_exclusive_guard_pauses_scheduled_work() {
    let owner = loac::spawn::<CxExclusiveCounter>(0);
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(CxExclusiveIncrement)).await, Ok(1));
    assert_eq!(watchdog(actor.call(CxExclusiveIncrement)).await, Ok(2));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct CxExclusiveStream(u8);

impl StreamHandler<CxExclusiveStream> for CxExclusiveCounter {
    async fn handle<'a, W>(
        message: CxExclusiveStream,
        mut out: StreamOut<'a, W>,
        mut cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        let mut guard = cx.exclusive();
        let base = message.0;
        let doubled = guard.with(|actor, _| {
            actor.0 += base;
            actor.0 * 2
        });
        let _ = out.write(doubled).await;
        doubled
    }
}

#[tokio::test]
async fn cx_stream_exclusive_writes_items_and_finishes() {
    let owner = loac::spawn::<CxExclusiveCounter>(0);
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(CxExclusiveStream(5)))
        .await
        .expect("the stream call commits");
    assert_eq!(watchdog(reply.recv()).await, Some(10));
    assert_eq!(watchdog(reply.recv()).await, None);
    assert_eq!(watchdog(reply.finish()).await, Ok(10));

    assert_eq!(watchdog(actor.call(CxExclusiveIncrement)).await, Ok(6));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}
