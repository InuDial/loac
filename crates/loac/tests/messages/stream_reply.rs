use futures_util::StreamExt;
use loac::{
    Actor, ActorScope, Cx, ExitReason, Message, Shutdown, StreamHandler, StreamOut, Writer, actor,
};

use super::support::watchdog;

struct StreamActor;

#[actor(mailbox)]
impl Actor for StreamActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct StreamNumbers(u8);

impl StreamHandler<StreamNumbers> for StreamActor {
    async fn handle<'a, W>(
        message: StreamNumbers,
        mut out: StreamOut<'a, W>,
        _cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        for item in 0..message.0 {
            if out.write(item).await.is_err() {
                break;
            }
        }
        message.0
    }
}

struct ExclusiveStreamActor;

#[actor(mailbox, interleaved)]
impl Actor for ExclusiveStreamActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct ExclusiveStreamNumbers(u8);

impl StreamHandler<ExclusiveStreamNumbers> for ExclusiveStreamActor {
    async fn handle<'a, W>(
        message: ExclusiveStreamNumbers,
        mut out: StreamOut<'a, W>,
        mut cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        let _guard = cx.exclusive();
        for item in 0..message.0 {
            if out.write(item).await.is_err() {
                break;
            }
        }
        message.0
    }
}

struct ConcurrentStreamActor;

#[actor(mailbox, interleaved)]
impl Actor for ConcurrentStreamActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct ConcurrentStreamNumbers(u8);

impl StreamHandler<ConcurrentStreamNumbers> for ConcurrentStreamActor {
    async fn handle<'a, W>(
        message: ConcurrentStreamNumbers,
        mut out: StreamOut<'a, W>,
        _cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        for item in 0..message.0 {
            if out.write(item).await.is_err() {
                break;
            }
        }
        message.0
    }
}

struct BranchStreamActor;

#[actor(mailbox)]
impl Actor for BranchStreamActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct BranchStream(u8);

impl StreamHandler<BranchStream> for BranchStreamActor {
    async fn handle<'a, W>(
        message: BranchStream,
        mut out: StreamOut<'a, W>,
        _cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        if message.0 > 0 {
            for item in 0..message.0 {
                if out.write(item).await.is_err() {
                    break;
                }
            }
        }
        message.0
    }
}

#[tokio::test]
async fn stream_handler_streams_items_and_finishes() {
    let owner = loac::spawn::<StreamActor>(());
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(StreamNumbers(3)))
        .await
        .expect("the stream call commits");

    assert_eq!(reply.recv().await, Some(0));
    assert_eq!(reply.recv().await, Some(1));
    assert_eq!(reply.recv().await, Some(2));
    assert_eq!(reply.recv().await, None);
    assert_eq!(reply.finish().await, Ok(3));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}

#[tokio::test]
async fn finish_discards_buffered_items_and_returns_final() {
    let owner = loac::spawn::<StreamActor>(());
    let actor = owner.actor_ref();

    let reply = watchdog(actor.call(StreamNumbers(3)))
        .await
        .expect("the stream call commits");

    assert_eq!(reply.finish().await, Ok(3));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}

#[tokio::test]
async fn items_view_borrows_without_losing_final() {
    let owner = loac::spawn::<StreamActor>(());
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(StreamNumbers(3)))
        .await
        .expect("the stream call commits");

    {
        let mut items = reply.items();
        assert_eq!(items.next().await, Some(0));
        assert_eq!(items.next().await, Some(1));
        assert_eq!(items.next().await, Some(2));
        assert_eq!(items.next().await, None);
    }

    assert_eq!(reply.finish().await, Ok(3));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}

#[tokio::test]
async fn exclusive_stream_handler_streams_and_finishes() {
    let owner = loac::spawn::<ExclusiveStreamActor>(());
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(ExclusiveStreamNumbers(3)))
        .await
        .expect("the stream call commits");

    assert_eq!(reply.recv().await, Some(0));
    assert_eq!(reply.recv().await, Some(1));
    assert_eq!(reply.recv().await, Some(2));
    assert_eq!(reply.recv().await, None);
    assert_eq!(reply.finish().await, Ok(3));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}

#[tokio::test]
async fn concurrent_stream_handler_streams_and_finishes() {
    let owner = loac::spawn::<ConcurrentStreamActor>(());
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(ConcurrentStreamNumbers(3)))
        .await
        .expect("the stream call commits");

    assert_eq!(reply.recv().await, Some(0));
    assert_eq!(reply.recv().await, Some(1));
    assert_eq!(reply.recv().await, Some(2));
    assert_eq!(reply.recv().await, None);
    assert_eq!(reply.finish().await, Ok(3));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}

#[tokio::test]
async fn stream_handler_chooses_a_runtime_branch() {
    let owner = loac::spawn::<BranchStreamActor>(());
    let actor = owner.actor_ref();

    let mut reply = watchdog(actor.call(BranchStream(0)))
        .await
        .expect("the stream call commits");
    assert_eq!(reply.recv().await, None);
    assert_eq!(reply.finish().await, Ok(0));

    let mut reply = watchdog(actor.call(BranchStream(2)))
        .await
        .expect("the stream call commits");
    assert_eq!(reply.recv().await, Some(0));
    assert_eq!(reply.recv().await, Some(1));
    assert_eq!(reply.recv().await, None);
    assert_eq!(reply.finish().await, Ok(2));

    let status = watchdog(owner.shutdown(Shutdown::Drain)).await;
    assert_eq!(status.reason(), ExitReason::Drained);
}
