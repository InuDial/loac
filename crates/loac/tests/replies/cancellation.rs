use super::*;

use tokio_util::sync::CancellationToken;

struct CancellationActor;

#[actor(mailbox, mailbox_capacity = 1, max_in_flight = 1)]
impl Actor for CancellationActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Debug, Eq, PartialEq)]
struct WorkCancelled;

#[derive(Message)]
#[message(reply = WorkCancelled)]
struct CancellableWork {
    started: oneshot::Sender<()>,
    cancellation: CancellationToken,
}

impl Handler<CancellableWork> for CancellationActor {
    async fn handle(message: CancellableWork, _cx: Cx<'_, Self>) -> WorkCancelled {
        let _ = message.started.send(());
        message.cancellation.cancelled().await;
        WorkCancelled
    }
}

#[derive(Message)]
#[message(reply = ())]
struct CancelWork {
    dispatched: oneshot::Sender<()>,
}

impl Handler<CancelWork> for CancellationActor {
    async fn handle(message: CancelWork, _cx: Cx<'_, Self>) {
        let _ = message.dispatched.send(());
    }
}

// The active handler occupies the only scheduler slot.
// Therefore, mailbox cancellation cannot reach that handler.
// A caller-owned token wakes it directly.
// The queued cancellation message can then dispatch.
#[tokio::test]
async fn caller_cancellation_bypasses_full_handler_capacity() {
    let owner = loac::spawn::<CancellationActor>(());
    let actor = owner.actor_ref();
    let cancellation = CancellationToken::new();
    let (started_tx, started_rx) = oneshot::channel();
    let active = actor
        .try_call(CancellableWork {
            started: started_tx,
            cancellation: cancellation.clone(),
        })
        .unwrap();

    watchdog(started_rx).await.unwrap();

    let (dispatched_tx, dispatched_rx) = oneshot::channel();
    let mut queued = Box::pin(
        actor
            .try_call(CancelWork {
                dispatched: dispatched_tx,
            })
            .unwrap(),
    );
    assert!(poll_once(queued.as_mut()).await.is_pending());

    cancellation.cancel();
    assert_eq!(watchdog(active).await, Ok(WorkCancelled));
    assert_eq!(watchdog(queued).await, Ok(()));
    watchdog(dispatched_rx).await.unwrap();

    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}
