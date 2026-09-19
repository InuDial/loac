use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use loac::{
    Actor, ActorScope, CallError, Cx, ExitReason, Handler, Message, Shutdown, ShutdownStatus,
    TryCallErrorKind, actor,
};
use tokio::sync::oneshot;

use super::{
    fixtures::{LifecycleActor, LifecycleHarness, Step, actor_with_capacity},
    support::{lock, watchdog},
};

#[derive(Message)]
#[message(reply = ())]
struct StopFromExclusive;

impl Handler<StopFromExclusive> for LifecycleActor {
    async fn handle(_message: StopFromExclusive, mut cx: Cx<'_, Self>) {
        let mut guard = cx.exclusive();
        guard.with(|_, scope| {
            assert_eq!(
                scope.request_shutdown(Shutdown::Stop),
                ShutdownStatus::Requested
            );
        });
    }
}

// Exclusive completion can commit graceful shutdown without a repoll.
#[tokio::test]
async fn exclusive_completion_can_commit_graceful_shutdown_without_repoll() {
    let LifecycleHarness {
        mut owner, cleanup, ..
    } = actor_with_capacity(1);
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(StopFromExclusive)).await, Ok(()));
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Stopped);
    assert_eq!(*lock(&cleanup), vec![ExitReason::Stopped]);
}

struct LeaseHookActor {
    phase: Arc<AtomicUsize>,
    observation: Option<oneshot::Sender<(Shutdown, usize)>>,
}

struct LeaseHookArgs {
    phase: Arc<AtomicUsize>,
    observation: oneshot::Sender<(Shutdown, usize)>,
}

#[actor(mailbox)]
impl Actor for LeaseHookActor {
    type SpawnArgs = LeaseHookArgs;

    async fn init(args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self {
            phase: args.phase,
            observation: Some(args.observation),
        }
    }

    fn on_shutdown(&mut self, shutdown: Shutdown) {
        let phase = self.phase.load(Ordering::SeqCst);
        if let Some(observation) = self.observation.take() {
            let _ = observation.send((shutdown, phase));
        }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct HoldLease {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<HoldLease> for LeaseHookActor {
    async fn handle(message: HoldLease, mut cx: Cx<'_, Self>) {
        let mut guard = cx.exclusive();
        guard.with(|actor, _| actor.phase.store(1, Ordering::SeqCst));
        let _ = message.entered.send(());
        let _ = message.release.await;
        guard.with(|actor, _| actor.phase.store(2, Ordering::SeqCst));
    }
}

async fn assert_shutdown_preempts_lease(shutdown: Shutdown, reason: ExitReason) {
    let phase = Arc::new(AtomicUsize::new(0));
    let (observation_tx, observation_rx) = oneshot::channel();
    let mut owner = loac::spawn::<LeaseHookActor>(LeaseHookArgs {
        phase: Arc::clone(&phase),
        observation: observation_tx,
    });
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let reply = tokio::spawn(async move {
        actor
            .call(HoldLease {
                entered: entered_tx,
                release: release_rx,
            })
            .await
    });

    watchdog(entered_rx).await.unwrap();
    assert_eq!(owner.request_shutdown(shutdown), ShutdownStatus::Requested);
    assert_eq!(watchdog(observation_rx).await.unwrap(), (shutdown, 1));

    release_tx.send(()).unwrap();
    assert_eq!(watchdog(reply).await.unwrap(), Ok(()));
    assert_eq!(watchdog(owner.wait()).await.reason(), reason);
    assert_eq!(phase.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stop_hook_preempts_an_exclusive_lease() {
    assert_shutdown_preempts_lease(Shutdown::Stop, ExitReason::Stopped).await;
}

#[tokio::test]
async fn drain_hook_preempts_an_exclusive_lease() {
    assert_shutdown_preempts_lease(Shutdown::Drain, ExitReason::Drained).await;
}

// Stop finishes the current message and cancels queued messages.
#[tokio::test]
async fn stop_finishes_current_and_cancels_queued_messages() {
    let LifecycleHarness {
        mut owner,
        handled,
        cleanup,
    } = actor_with_capacity(2);
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let current = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .call(Step {
                    id: 1,
                    entered: Some(entered_tx),
                    release: Some(release_rx),
                })
                .await
        }
    });

    watchdog(entered_rx).await.unwrap();
    let queued = actor.try_call(Step::immediate(2)).unwrap();
    assert_eq!(
        owner.request_shutdown(Shutdown::Stop),
        ShutdownStatus::Requested
    );
    assert_eq!(
        actor.try_call(Step::immediate(3)).unwrap_err().kind(),
        TryCallErrorKind::Closed
    );

    release_tx.send(()).unwrap();
    assert_eq!(watchdog(current).await.unwrap(), Ok(1));
    assert_eq!(
        watchdog(queued).await,
        Err(CallError::BeforeDispatch(ExitReason::Stopped))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Stopped);
    assert_eq!(*lock(&handled), vec![1]);
    assert_eq!(*lock(&cleanup), vec![ExitReason::Stopped]);
}

// Drain runs the fixed accepted queue in admission order.
#[tokio::test]
async fn drain_runs_the_fixed_accepted_queue_in_order() {
    let LifecycleHarness {
        mut owner,
        handled,
        cleanup,
    } = actor_with_capacity(3);
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let current = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .call(Step {
                    id: 1,
                    entered: Some(entered_tx),
                    release: Some(release_rx),
                })
                .await
        }
    });

    watchdog(entered_rx).await.unwrap();
    let second = actor.try_call(Step::immediate(2)).unwrap();
    let third = actor.try_call(Step::immediate(3)).unwrap();
    assert_eq!(
        owner.request_shutdown(Shutdown::Drain),
        ShutdownStatus::Requested
    );
    assert_eq!(
        actor.try_call(Step::immediate(4)).unwrap_err().kind(),
        TryCallErrorKind::Closed
    );

    release_tx.send(()).unwrap();
    assert_eq!(watchdog(current).await.unwrap(), Ok(1));
    assert_eq!(watchdog(second).await, Ok(2));
    assert_eq!(watchdog(third).await, Ok(3));
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Drained);
    assert_eq!(*lock(&handled), vec![1, 2, 3]);
    assert_eq!(*lock(&cleanup), vec![ExitReason::Drained]);
}

struct ConcurrentDrainActor;

#[actor(mailbox, mailbox_capacity = 3, max_in_flight = 2)]
impl Actor for ConcurrentDrainActor {
    type SpawnArgs = ();

    async fn init(_args: (), _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct ConcurrentDrainStep {
    id: u8,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<ConcurrentDrainStep> for ConcurrentDrainActor {
    async fn handle(message: ConcurrentDrainStep, _cx: Cx<'_, Self>) -> u8 {
        let _ = message.entered.send(());
        let _ = message.release.await;
        message.id
    }
}

// Drain respects max_in_flight for scheduled replies.
#[tokio::test]
async fn drain_respects_the_fixed_max_in_flight_limit() {
    let mut owner = loac::spawn::<ConcurrentDrainActor>(());
    let actor = owner.actor_ref();

    let (first_entered_tx, first_entered_rx) = oneshot::channel();
    let (first_release_tx, first_release_rx) = oneshot::channel();
    let first = actor
        .try_call(ConcurrentDrainStep {
            id: 1,
            entered: first_entered_tx,
            release: first_release_rx,
        })
        .unwrap();
    let (second_entered_tx, second_entered_rx) = oneshot::channel();
    let (second_release_tx, second_release_rx) = oneshot::channel();
    let second = actor
        .try_call(ConcurrentDrainStep {
            id: 2,
            entered: second_entered_tx,
            release: second_release_rx,
        })
        .unwrap();
    watchdog(first_entered_rx).await.unwrap();
    watchdog(second_entered_rx).await.unwrap();

    let (third_entered_tx, mut third_entered_rx) = oneshot::channel();
    let (third_release_tx, third_release_rx) = oneshot::channel();
    let third = actor
        .try_call(ConcurrentDrainStep {
            id: 3,
            entered: third_entered_tx,
            release: third_release_rx,
        })
        .unwrap();

    assert_eq!(
        owner.request_shutdown(Shutdown::Drain),
        ShutdownStatus::Requested
    );
    let (fourth_entered_tx, _fourth_entered_rx) = oneshot::channel();
    let (_fourth_release_tx, fourth_release_rx) = oneshot::channel();
    assert_eq!(
        actor
            .try_call(ConcurrentDrainStep {
                id: 4,
                entered: fourth_entered_tx,
                release: fourth_release_rx,
            })
            .unwrap_err()
            .kind(),
        TryCallErrorKind::Closed
    );
    assert!(matches!(
        third_entered_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    first_release_tx.send(()).unwrap();
    watchdog(third_entered_rx).await.unwrap();
    second_release_tx.send(()).unwrap();
    third_release_tx.send(()).unwrap();

    assert_eq!(watchdog(first).await, Ok(1));
    assert_eq!(watchdog(second).await, Ok(2));
    assert_eq!(watchdog(third).await, Ok(3));
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Drained);
}
