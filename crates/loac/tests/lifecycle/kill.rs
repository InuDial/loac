use std::{future, sync::mpsc as std_mpsc, time::Duration};

use loac::{CallError, Cx, ExitReason, Handler, Message, Shutdown, ShutdownStatus};
use tokio::sync::oneshot;

use super::{
    fixtures::{DropSignal, LifecycleActor, LifecycleHarness, Step, actor_with_capacity},
    support::{lock, watchdog},
};

#[derive(Message)]
#[message(reply = ())]
struct Interruptible {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    dropped: DropSignal,
}

impl Handler<Interruptible> for LifecycleActor {
    async fn handle(message: Interruptible, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _dropped = message.dropped;
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ForgottenLease {
    entered: oneshot::Sender<()>,
}

impl Handler<ForgottenLease> for LifecycleActor {
    async fn handle(message: ForgottenLease, mut cx: Cx<'_, Self>) {
        let guard = cx.exclusive();
        std::mem::forget(guard);
        let _ = message.entered.send(());
        future::pending().await
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ScheduledInterruptible {
    entered: oneshot::Sender<()>,
    drop_barrier: DropBarrier,
}

struct DropBarrier {
    entered: Option<oneshot::Sender<()>>,
    release: std_mpsc::Receiver<()>,
}

impl Drop for DropBarrier {
    fn drop(&mut self) {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        let _ = self.release.recv();
    }
}

impl Handler<ScheduledInterruptible> for LifecycleActor {
    async fn handle(message: ScheduledInterruptible, _cx: Cx<'_, Self>) {
        let _drop_barrier = message.drop_barrier;
        let _ = message.entered.send(());
        future::pending().await
    }
}

#[derive(Message)]
#[message(reply = ())]
struct KillBeforeReady;

impl Handler<KillBeforeReady> for LifecycleActor {
    async fn handle(_message: KillBeforeReady, mut cx: Cx<'_, Self>) {
        cx.with(|_, scope| {
            assert_eq!(
                scope.request_shutdown(Shutdown::Kill),
                ShutdownStatus::Requested
            );
        });
    }
}

// Kill before completion reports the dispatching phase.
#[tokio::test]
async fn kill_before_completion_reports_the_dispatching_phase() {
    let mut owner = actor_with_capacity(1).owner;
    let actor = owner.actor_ref();

    assert_eq!(
        watchdog(actor.call(KillBeforeReady)).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
}

// Kill drops current and queued work without graceful cleanup.
#[tokio::test]
async fn kill_drops_current_and_queued_work_without_cleanup() {
    let LifecycleHarness {
        mut owner,
        handled,
        cleanup,
    } = actor_with_capacity(2);
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let current = actor
        .try_call(Interruptible {
            entered: entered_tx,
            release: release_rx,
            dropped: DropSignal(Some(dropped_tx)),
        })
        .unwrap();

    watchdog(entered_rx).await.unwrap();
    let queued = actor.try_call(Step::immediate(2)).unwrap();
    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        ShutdownStatus::Requested
    );

    watchdog(dropped_rx).await.unwrap();
    assert_eq!(
        watchdog(current).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(
        watchdog(queued).await,
        Err(CallError::BeforeDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
    assert!(lock(&handled).is_empty());
    assert!(lock(&cleanup).is_empty());
}

#[tokio::test]
async fn kill_terminates_a_reply_with_a_forgotten_lease() {
    let mut owner = actor_with_capacity(1).owner;
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let response = actor
        .try_call(ForgottenLease {
            entered: entered_tx,
        })
        .unwrap();

    watchdog(entered_rx).await.unwrap();
    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        ShutdownStatus::Requested
    );

    assert_eq!(
        watchdog(response).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
}

// The destructor blocks during scheduler cleanup.
// Kill waits before publishing exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_drops_scheduled_reply_before_publishing_exit() {
    let mut owner = actor_with_capacity(1).owner;
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (drop_entered_tx, drop_entered_rx) = oneshot::channel();
    let (drop_release_tx, drop_release_rx) = std_mpsc::channel();
    let response = actor
        .try_call(ScheduledInterruptible {
            entered: entered_tx,
            drop_barrier: DropBarrier {
                entered: Some(drop_entered_tx),
                release: drop_release_rx,
            },
        })
        .unwrap();
    watchdog(entered_rx).await.unwrap();

    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        ShutdownStatus::Requested
    );
    let wait = owner.wait();
    tokio::pin!(wait);
    watchdog(drop_entered_rx).await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(20), wait.as_mut()).await;
    drop_release_tx.send(()).unwrap();

    assert!(early.is_err());
    assert_eq!(watchdog(wait).await.reason(), ExitReason::Killed);
    assert_eq!(
        watchdog(response).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
}

// Graceful mode is first-wins and Kill can upgrade it.
#[tokio::test]
async fn graceful_mode_is_first_wins_and_kill_can_upgrade_it() {
    let LifecycleHarness {
        mut owner,
        handled: _,
        cleanup,
    } = actor_with_capacity(1);
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let current = actor
        .try_call(Interruptible {
            entered: entered_tx,
            release: release_rx,
            dropped: DropSignal(Some(dropped_tx)),
        })
        .unwrap();
    watchdog(entered_rx).await.unwrap();

    assert_eq!(
        owner.request_shutdown(Shutdown::Drain),
        ShutdownStatus::Requested
    );
    assert_eq!(
        owner.request_shutdown(Shutdown::Stop),
        ShutdownStatus::InProgress(Shutdown::Drain)
    );
    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        ShutdownStatus::Requested
    );

    watchdog(dropped_rx).await.unwrap();
    assert_eq!(
        watchdog(current).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
    assert!(lock(&cleanup).is_empty());
}
