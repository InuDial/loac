use super::*;

struct PanicActor;

#[actor(mailbox, interleaved = 2)]
impl Actor for PanicActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct CxPanicActor;

#[actor(mailbox, interleaved)]
impl Actor for CxPanicActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PanicAfterLease;

impl Handler<PanicAfterLease> for CxPanicActor {
    async fn handle(_message: PanicAfterLease, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        panic!("intentional panic after lease acquisition");
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PendingSibling {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<PendingSibling> for PanicActor {
    async fn handle(message: PendingSibling, _cx: Cx<'_, Self>) {
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PanicReply {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[derive(Message)]
#[message(reply = ())]
struct PanicAfterReady;

impl Future for PanicAfterReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

impl Drop for PanicAfterReady {
    fn drop(&mut self) {
        panic!("intentional post-completion drop panic");
    }
}

impl Handler<PanicAfterReady> for PanicActor {
    fn handle(message: PanicAfterReady, _cx: Cx<'_, Self>) -> impl Future<Output = ()> + Send + '_ {
        message
    }
}

impl Handler<PanicReply> for PanicActor {
    async fn handle(message: PanicReply, _cx: Cx<'_, Self>) {
        let _ = message.entered.send(());
        let _ = message.release.await;
        panic!("intentional reply panic");
    }
}

#[tokio::test]
async fn reply_panic_fails_sibling_in_flight_work() {
    let mut owner = loac::spawn::<PanicActor>(());
    let actor = owner.actor_ref();
    let (sibling_entered_tx, sibling_entered_rx) = oneshot::channel();
    let (_sibling_release_tx, sibling_release_rx) = oneshot::channel();
    let sibling = actor
        .try_call(PendingSibling {
            entered: sibling_entered_tx,
            release: sibling_release_rx,
        })
        .unwrap();
    watchdog(sibling_entered_rx).await.unwrap();

    let (panic_entered_tx, panic_entered_rx) = oneshot::channel();
    let (panic_release_tx, panic_release_rx) = oneshot::channel();
    let panicking = actor
        .try_call(PanicReply {
            entered: panic_entered_tx,
            release: panic_release_rx,
        })
        .unwrap();
    watchdog(panic_entered_rx).await.unwrap();
    panic_release_tx.send(()).unwrap();

    assert_eq!(
        watchdog(panicking).await,
        Err(CallError::DuringDispatch(ExitReason::Panicked))
    );
    assert_eq!(
        watchdog(sibling).await,
        Err(CallError::DuringDispatch(ExitReason::Panicked))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Panicked);
}

#[tokio::test]
async fn panic_after_lease_acquisition_fails_the_actor() {
    let mut owner = loac::spawn::<CxPanicActor>(());
    let actor = owner.actor_ref();

    assert_eq!(
        watchdog(actor.call(PanicAfterLease)).await,
        Err(CallError::DuringDispatch(ExitReason::Panicked))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Panicked);
}

// Reply completion consumes its lifecycle gate first.
// A later future Drop panic must still fail the actor.
#[tokio::test]
async fn future_drop_panic_after_reply_completion_still_fails_the_actor() {
    let mut owner = loac::spawn::<PanicActor>(());
    let actor = owner.actor_ref();

    assert_eq!(watchdog(actor.call(PanicAfterReady)).await, Ok(()));
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Panicked);
}
