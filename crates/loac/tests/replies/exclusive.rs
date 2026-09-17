use super::*;

struct ExclusiveActorArgs {
    child_started: oneshot::Sender<ActorRef<HookChild>>,
    child_hooks: mpsc::UnboundedSender<()>,
}

struct ExclusiveActor {
    child_hooks: mpsc::UnboundedSender<()>,
}

#[actor(mailbox, children = unbounded, interleaved)]
impl Actor for ExclusiveActor {
    type SpawnArgs = ExclusiveActorArgs;

    async fn init(args: Self::SpawnArgs, scope: &mut ActorScope<'_, Self>) -> Self {
        let Ok(child) = scope.spawn_child::<HookChild>(());
        let child = child.into_actor_ref();
        let _ = args.child_started.send(child);
        Self {
            child_hooks: args.child_hooks,
        }
    }

    async fn on_child_exit(&mut self, _event: ChildExit, _scope: &mut ActorScope<'_, Self>) {
        let _ = self.child_hooks.send(());
    }
}

impl DispatchHandler<PendingOwned> for ExclusiveActor {
    fn handle(
        &mut self,
        message: PendingOwned,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, PendingOwned> {
        async move {
            let _ = message.entered.send(());
            let _ = message.release.await;
        }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct InterleavedGate {
    dispatched: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl DispatchHandler<InterleavedGate> for ExclusiveActor {
    fn handle(
        &mut self,
        message: InterleavedGate,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, InterleavedGate> {
        let _ = message.dispatched.send(());
        async move { drop(message.release.await) }.interleaved()
    }
}

impl Handler<ExclusiveGate> for ExclusiveActor {
    async fn handle(message: ExclusiveGate, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Mark(oneshot::Sender<()>);

impl DispatchHandler<Mark> for ExclusiveActor {
    fn handle(
        &mut self,
        message: Mark,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, Mark> {
        let _ = message.0.send(());
        ().ready()
    }
}

#[tokio::test]
async fn exclusive_blocks_actor_work_but_owned_continues() {
    let (child_started_tx, child_started_rx) = oneshot::channel();
    let (child_hooks_tx, mut child_hooks_rx) = mpsc::unbounded_channel();
    let owner = loac::spawn::<ExclusiveActor>(ExclusiveActorArgs {
        child_started: child_started_tx,
        child_hooks: child_hooks_tx,
    });
    let actor = owner.actor_ref();
    let child = watchdog(child_started_rx).await.unwrap();

    let (owned_entered_tx, owned_entered_rx) = oneshot::channel();
    let (owned_release_tx, owned_release_rx) = oneshot::channel();
    let owned = actor
        .try_call(PendingOwned {
            entered: owned_entered_tx,
            release: owned_release_rx,
        })
        .unwrap();
    watchdog(owned_entered_rx).await.unwrap();

    let (interleaved_dispatched_tx, interleaved_dispatched_rx) = oneshot::channel();
    let (interleaved_release_tx, interleaved_release_rx) = oneshot::channel();
    let mut interleaved = Box::pin(
        actor
            .try_call(InterleavedGate {
                dispatched: interleaved_dispatched_tx,
                release: interleaved_release_rx,
            })
            .unwrap(),
    );
    watchdog(interleaved_dispatched_rx).await.unwrap();

    let (exclusive_entered_tx, exclusive_entered_rx) = oneshot::channel();
    let (exclusive_release_tx, exclusive_release_rx) = oneshot::channel();
    let exclusive = actor
        .try_call(ExclusiveGate {
            entered: exclusive_entered_tx,
            release: exclusive_release_rx,
        })
        .unwrap();
    watchdog(exclusive_entered_rx).await.unwrap();

    let (marked_tx, marked_rx) = oneshot::channel();
    let mut later_message = Box::pin(actor.try_call(Mark(marked_tx)).unwrap());
    assert_eq!(watchdog(child.call(StopChild)).await, Ok(()));
    assert_eq!(watchdog(child.closed()).await.reason(), ExitReason::Stopped);

    interleaved_release_tx.send(()).unwrap();
    owned_release_tx.send(()).unwrap();
    assert_eq!(watchdog(owned).await, Ok(()));

    assert!(poll_once(interleaved.as_mut()).await.is_pending());
    assert!(poll_once(later_message.as_mut()).await.is_pending());
    assert!(child_hooks_rx.try_recv().is_err());

    exclusive_release_tx.send(()).unwrap();
    assert_eq!(watchdog(exclusive).await, Ok(()));
    assert_eq!(watchdog(interleaved).await, Ok(()));
    assert_eq!(watchdog(later_message).await, Ok(()));
    watchdog(marked_rx).await.unwrap();
    watchdog(child_hooks_rx.recv()).await.unwrap();

    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

struct ScopedLeaseActor;

#[actor(mailbox = 4, interleaved = 2)]
impl Actor for ScopedLeaseActor {
    type SpawnArgs = ();

    async fn init(_args: (), _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ScopedLease {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    released: oneshot::Sender<()>,
    finish: oneshot::Receiver<()>,
}

impl Handler<ScopedLease> for ScopedLeaseActor {
    async fn handle(message: ScopedLease, mut cx: Cx<'_, Self>) {
        let guard = cx.exclusive();
        let _ = message.entered.send(());
        let _ = message.release.await;
        drop(guard);
        let _ = message.released.send(());
        let _ = message.finish.await;
    }
}

#[derive(Message)]
#[message(reply = ())]
struct LeaseMark(oneshot::Sender<()>);

impl DispatchHandler<LeaseMark> for ScopedLeaseActor {
    fn handle(
        &mut self,
        message: LeaseMark,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, LeaseMark> {
        let _ = message.0.send(());
        ().ready()
    }
}

#[tokio::test]
async fn dropping_the_guard_resumes_actor_work_before_reply_completion() {
    let owner = loac::spawn::<ScopedLeaseActor>(());
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (released_tx, released_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let mut leased = Box::pin(
        actor
            .try_call(ScopedLease {
                entered: entered_tx,
                release: release_rx,
                released: released_tx,
                finish: finish_rx,
            })
            .unwrap(),
    );
    watchdog(entered_rx).await.unwrap();

    let (marked_tx, marked_rx) = oneshot::channel();
    let mut marked = Box::pin(actor.try_call(LeaseMark(marked_tx)).unwrap());
    assert!(poll_once(marked.as_mut()).await.is_pending());

    release_tx.send(()).unwrap();
    watchdog(released_rx).await.unwrap();
    assert_eq!(watchdog(marked.as_mut()).await, Ok(()));
    watchdog(marked_rx).await.unwrap();
    assert!(poll_once(leased.as_mut()).await.is_pending());

    finish_tx.send(()).unwrap();
    assert_eq!(watchdog(leased).await, Ok(()));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}
