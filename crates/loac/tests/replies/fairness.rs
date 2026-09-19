use super::*;

struct FairActor {
    handled: Arc<AtomicUsize>,
}

#[actor(mailbox = 64, interleaved)]
impl Actor for FairActor {
    type SpawnArgs = Arc<AtomicUsize>;

    async fn init(handled: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self { handled }
    }
}

#[derive(Message)]
#[message(reply = bool)]
struct ActorTaskIdentity;

impl Handler<ActorTaskIdentity> for FairActor {
    async fn handle(_message: ActorTaskIdentity, _cx: Cx<'_, Self>) -> bool {
        let actor_task = tokio::task::id();
        tokio::task::yield_now().await;
        tokio::task::id() == actor_task
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ActiveReply {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    completed_at: Arc<AtomicUsize>,
}

impl Handler<ActiveReply> for FairActor {
    async fn handle(message: ActiveReply, mut cx: Cx<'_, Self>) {
        let _ = message.entered.send(());
        let _ = message.release.await;
        cx.with(|actor, _| {
            message
                .completed_at
                .store(actor.handled.load(Ordering::SeqCst), Ordering::SeqCst);
        });
    }
}

#[derive(Message)]
#[message(reply = ())]
struct MailboxWork;

impl Handler<MailboxWork> for FairActor {
    async fn handle(_message: MailboxWork, mut cx: Cx<'_, Self>) {
        cx.with(|actor, _| {
            actor.handled.fetch_add(1, Ordering::SeqCst);
        });
    }
}

#[tokio::test]
async fn reply_stays_on_its_actor_task() {
    let handled = Arc::new(AtomicUsize::new(0));
    let owner = loac::spawn::<FairActor>(handled.clone());
    let actor = owner.actor_ref();
    assert_eq!(watchdog(actor.call(ActorTaskIdentity)).await, Ok(true));
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

#[tokio::test]
async fn mailbox_input_does_not_starve_woken_reply() {
    let handled = Arc::new(AtomicUsize::new(0));
    let owner = loac::spawn::<FairActor>(handled.clone());
    let actor = owner.actor_ref();
    let completed_at = Arc::new(AtomicUsize::new(usize::MAX));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let active = actor
        .try_call(ActiveReply {
            entered: entered_tx,
            release: release_rx,
            completed_at: completed_at.clone(),
        })
        .unwrap();
    tokio::pin!(active);
    watchdog(entered_rx).await.unwrap();
    assert!(poll_once(active.as_mut()).await.is_pending());

    let queued: Vec<_> = (0..32)
        .map(|_| actor.try_call(MailboxWork).unwrap())
        .collect();
    release_tx.send(()).unwrap();
    assert_eq!(watchdog(active).await, Ok(()));
    assert!(completed_at.load(Ordering::SeqCst) < queued.len());

    for response in queued {
        assert_eq!(watchdog(response).await, Ok(()));
    }
    assert_eq!(handled.load(Ordering::SeqCst), 32);
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}

struct FairChildExitArgs {
    child_started: oneshot::Sender<ActorRef<HookChild>>,
    handled: Arc<AtomicUsize>,
    hook_completed_at: Arc<AtomicUsize>,
    hook_completed: oneshot::Sender<()>,
}

struct FairChildExitActor {
    handled: Arc<AtomicUsize>,
    hook_completed_at: Arc<AtomicUsize>,
    hook_completed: Option<oneshot::Sender<()>>,
}

#[actor(mailbox = 64, interleaved, children = unbounded)]
impl Actor for FairChildExitActor {
    type SpawnArgs = FairChildExitArgs;

    async fn init(args: Self::SpawnArgs, scope: &mut ActorScope<'_, Self>) -> Self {
        let Ok(child) = scope.spawn_child::<HookChild>(());
        let child = child.into_actor_ref();
        let _ = args.child_started.send(child);
        Self {
            handled: args.handled,
            hook_completed_at: args.hook_completed_at,
            hook_completed: Some(args.hook_completed),
        }
    }

    async fn on_child_exit(&mut self, _event: ChildExit, _scope: &mut ActorScope<'_, Self>) {
        self.hook_completed_at
            .store(self.handled.load(Ordering::SeqCst), Ordering::SeqCst);
        if let Some(completed) = self.hook_completed.take() {
            let _ = completed.send(());
        }
    }
}

impl Handler<PendingReply> for FairChildExitActor {
    async fn handle(message: PendingReply, _cx: Cx<'_, Self>) {
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

impl Handler<ExclusiveGate> for FairChildExitActor {
    async fn handle(message: ExclusiveGate, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

impl Handler<MailboxWork> for FairChildExitActor {
    async fn handle(_message: MailboxWork, mut cx: Cx<'_, Self>) {
        cx.with(|actor, _| {
            actor.handled.fetch_add(1, Ordering::SeqCst);
        });
    }
}

#[tokio::test(flavor = "current_thread")]
async fn queued_child_exit_progresses_before_mailbox_is_exhausted() {
    let handled = Arc::new(AtomicUsize::new(0));
    let hook_completed_at = Arc::new(AtomicUsize::new(usize::MAX));
    let (child_started_tx, child_started_rx) = oneshot::channel();
    let (hook_completed_tx, hook_completed_rx) = oneshot::channel();
    let owner = loac::spawn::<FairChildExitActor>(FairChildExitArgs {
        child_started: child_started_tx,
        handled: handled.clone(),
        hook_completed_at: hook_completed_at.clone(),
        hook_completed: hook_completed_tx,
    });
    let actor = owner.actor_ref();
    let child = watchdog(child_started_rx).await.unwrap();

    let (pending_entered_tx, pending_entered_rx) = oneshot::channel();
    let (pending_release_tx, pending_release_rx) = oneshot::channel();
    let pending = actor
        .try_call(PendingReply {
            entered: pending_entered_tx,
            release: pending_release_rx,
        })
        .unwrap();
    watchdog(pending_entered_rx).await.unwrap();

    let (exclusive_entered_tx, exclusive_entered_rx) = oneshot::channel();
    let (exclusive_release_tx, exclusive_release_rx) = oneshot::channel();
    let exclusive = actor
        .try_call(ExclusiveGate {
            entered: exclusive_entered_tx,
            release: exclusive_release_rx,
        })
        .unwrap();
    watchdog(exclusive_entered_rx).await.unwrap();

    assert_eq!(watchdog(child.call(StopChild)).await, Ok(()));
    // On the current-thread runtime the child publishes its supervisor event
    // before this task resumes; exclusive keeps the parent from consuming it.
    assert_eq!(watchdog(child.closed()).await.reason(), ExitReason::Stopped);
    let mut hook_completed = Box::pin(hook_completed_rx);
    assert!(poll_once(hook_completed.as_mut()).await.is_pending());

    let queued: Vec<_> = (0..32)
        .map(|_| actor.try_call(MailboxWork).unwrap())
        .collect();
    exclusive_release_tx.send(()).unwrap();
    assert_eq!(watchdog(exclusive).await, Ok(()));
    watchdog(hook_completed).await.unwrap();
    assert!(hook_completed_at.load(Ordering::SeqCst) < queued.len());

    pending_release_tx.send(()).unwrap();
    assert_eq!(watchdog(pending).await, Ok(()));
    for response in queued {
        assert_eq!(watchdog(response).await, Ok(()));
    }
    assert_eq!(handled.load(Ordering::SeqCst), 32);
    assert_eq!(
        watchdog(owner.shutdown(Shutdown::Stop)).await.reason(),
        ExitReason::Stopped
    );
}
