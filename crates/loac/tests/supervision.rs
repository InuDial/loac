mod support;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Poll;

use loac::{
    Actor, ActorScope, CallError, Child, ChildExit, Cx, ExitReason, Handler, Message, Shutdown,
    SubtreeStatus, actor,
};
use tokio::sync::{mpsc, oneshot};

use support::watchdog;

struct ChildActor;

#[actor(mailbox)]
impl Actor for ChildActor {
    type SpawnArgs = bool;

    async fn init(exits_during_init: Self::SpawnArgs, scope: &mut ActorScope<'_, Self>) -> Self {
        if exits_during_init {
            let _ = scope.request_shutdown(Shutdown::Stop);
        }
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct StopSelf;

impl Handler<StopSelf> for ChildActor {
    async fn handle(_message: StopSelf, mut cx: Cx<'_, Self>) {
        cx.with(|_, scope| {
            let _ = scope.request_shutdown(Shutdown::Stop);
        });
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PanicSelf;

impl Handler<PanicSelf> for ChildActor {
    async fn handle(_message: PanicSelf, _cx: Cx<'_, Self>) {
        panic!("intentional child panic");
    }
}

struct Supervisor {
    events: mpsc::UnboundedSender<ChildExit>,
    observed: Arc<AtomicUsize>,
}

struct SupervisorArgs {
    child_started: oneshot::Sender<Child<ChildActor>>,
    events: mpsc::UnboundedSender<ChildExit>,
    observed: Arc<AtomicUsize>,
    child_exits_during_init: bool,
}

#[actor(mailbox, children, max_children = unbounded)]
impl Actor for Supervisor {
    type SpawnArgs = SupervisorArgs;

    async fn init(args: Self::SpawnArgs, scope: &mut ActorScope<'_, Self>) -> Self {
        let Ok(child) = scope.spawn_child::<ChildActor>(args.child_exits_during_init);
        let _ = args.child_started.send(child);
        Self {
            events: args.events,
            observed: args.observed,
        }
    }

    async fn on_child_exit<'a>(
        &'a mut self,
        event: ChildExit,
        _scope: &'a mut ActorScope<'_, Self>,
    ) {
        self.observed.fetch_add(1, Ordering::SeqCst);
        let _ = self.events.send(event);
    }
}

#[derive(Message)]
#[message(reply = usize)]
struct Observed;

impl Handler<Observed> for Supervisor {
    async fn handle(_message: Observed, mut cx: Cx<'_, Self>) -> usize {
        cx.with(|actor, _| actor.observed.load(Ordering::SeqCst))
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ChildExitBarrier;

impl Handler<ChildExitBarrier> for Supervisor {
    async fn handle(_message: ChildExitBarrier, _cx: Cx<'_, Self>) {
        let mut yielded = false;
        std::future::poll_fn(move |task| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                task.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await
    }
}

fn spawn_supervisor(
    child_exits_during_init: bool,
) -> (
    loac::ActorOwner<Supervisor>,
    oneshot::Receiver<Child<ChildActor>>,
    mpsc::UnboundedReceiver<ChildExit>,
) {
    let (child_tx, child_rx) = oneshot::channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let owner = loac::spawn::<Supervisor>(SupervisorArgs {
        child_started: child_tx,
        events: events_tx,
        observed: Arc::new(AtomicUsize::new(0)),
        child_exits_during_init,
    });
    (owner, child_rx, events_rx)
}

// A clean child exit is reported exactly once with its identity.
#[tokio::test(flavor = "current_thread")]
async fn clean_child_exit_is_reported_exactly_once() {
    let (owner, child_rx, mut events) = spawn_supervisor(false);
    let supervisor = owner.actor_ref();
    let child = watchdog(child_rx).await.unwrap();

    assert_eq!(watchdog(child.call(StopSelf)).await, Ok(()));
    let event = watchdog(events.recv()).await.unwrap();
    // The spawn receipt must identify its matching parent event.
    assert_eq!(event.child(), child.id());
    assert_eq!(event.status().reason(), ExitReason::Stopped);
    assert_eq!(watchdog(child.closed()).await.reason(), ExitReason::Stopped);

    // The barrier remains pending across a child-exit scheduling opportunity, so
    // a duplicate queued behind the first hook cannot hide behind mailbox work.
    assert_eq!(watchdog(supervisor.call(ChildExitBarrier)).await, Ok(()));
    assert_eq!(watchdog(supervisor.call(Observed)).await.unwrap(), 1);
    assert!(events.try_recv().is_err());
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

// The earliest normal exit path must preserve identity correlation.
// Multiple workers permit child exit before the receipt arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_exiting_during_init_keeps_its_registered_identity() {
    let (owner, child_rx, mut events) = spawn_supervisor(true);
    let child = watchdog(child_rx).await.unwrap();
    let event = watchdog(events.recv()).await.unwrap();

    assert_eq!(event.child(), child.id());
    assert_eq!(event.status().reason(), ExitReason::Stopped);
    assert_eq!(watchdog(child.closed()).await, event.status());
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

// Child panic is contained and reported without stopping the parent.
#[tokio::test]
async fn child_panic_is_reported_without_stopping_the_parent() {
    let (owner, child_rx, mut events) = spawn_supervisor(false);
    let supervisor = owner.actor_ref();
    let child = watchdog(child_rx).await.unwrap();

    assert_eq!(
        watchdog(child.call(PanicSelf)).await,
        Err(CallError::DuringDispatch(ExitReason::Panicked))
    );
    let event = watchdog(events.recv()).await.unwrap();
    // Panic changes status, not the child-to-event identity binding.
    assert_eq!(event.child(), child.id());
    let child_status = event.status();
    assert_eq!(child_status.reason(), ExitReason::Panicked);
    assert_eq!(child_status.subtree(), SubtreeStatus::Terminated);
    assert_eq!(watchdog(child.closed()).await, child_status);
    assert_eq!(watchdog(supervisor.call(Observed)).await.unwrap(), 1);

    let parent_status = watchdog(owner.shutdown(Shutdown::Stop)).await;
    assert_eq!(parent_status.reason(), ExitReason::Stopped);
    assert_eq!(parent_status.subtree(), SubtreeStatus::Terminated);
}

// An external child Kill disposes the child independently of its parent.
#[tokio::test]
async fn child_kill_is_reported_without_stopping_the_parent() {
    let (owner, child_rx, mut events) = spawn_supervisor(false);
    let supervisor = owner.actor_ref();
    let child = watchdog(child_rx).await.unwrap();

    assert_eq!(
        child.request_shutdown(Shutdown::Kill),
        loac::ShutdownStatus::Requested
    );
    let event = watchdog(events.recv()).await.unwrap();
    assert_eq!(event.child(), child.id());
    assert_eq!(event.status().reason(), ExitReason::Killed);
    assert_eq!(watchdog(child.closed()).await.reason(), ExitReason::Killed);
    assert_eq!(watchdog(supervisor.call(Observed)).await.unwrap(), 1);

    let parent_status = watchdog(owner.shutdown(Shutdown::Stop)).await;
    assert_eq!(parent_status.reason(), ExitReason::Stopped);
    assert_eq!(parent_status.subtree(), SubtreeStatus::Terminated);
}
