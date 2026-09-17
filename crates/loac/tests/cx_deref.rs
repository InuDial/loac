use std::future::Future;

use loac::{Actor, ActorScope, Cx, ExitReason, Handler, Message, Shutdown, actor};
use tokio::sync::oneshot;

struct Worker;

#[actor(mailbox, interleaved = unbounded)]
impl Actor for Worker {
    type SpawnArgs = ();

    async fn init((): (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Ping;

impl Handler<Ping> for Worker {
    async fn handle(_message: Ping, cx: Cx<'_, Self>) {
        // `Cx::myself` returns the same address the scope would provide.
        assert!(cx.myself().exit_status().is_none());
        // `Deref` makes ActorRef methods callable directly on `cx`.
        assert!(cx.exit_status().is_none());
    }
}

#[derive(Message)]
#[message(reply = ())]
struct HoldAddress {
    held: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<HoldAddress> for Worker {
    async fn handle(message: HoldAddress, cx: Cx<'_, Self>) {
        let address = cx.myself();
        message.held.send(()).expect("holder should be observed");
        message.release.await.expect("holder should be released");
        assert!(address.exit_status().is_none());
    }
}

#[derive(Message)]
#[message(reply = ())]
struct OpenScope;

impl Handler<OpenScope> for Worker {
    async fn handle(_message: OpenScope, mut cx: Cx<'_, Self>) {
        cx.with(|_, scope| assert!(scope.myself().exit_status().is_none()));
    }
}

#[derive(Message)]
#[message(reply = ())]
struct EagerAccess;

impl Handler<EagerAccess> for Worker {
    fn handle<'a>(
        _message: EagerAccess,
        mut cx: Cx<'a, Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        cx.with(|_actor, scope| {
            assert!(scope.myself().exit_status().is_none());
        });
        std::future::ready(())
    }
}

#[tokio::test]
async fn cx_exposes_the_actor_ref() {
    let owner = loac::spawn::<Worker>(());

    owner.call(Ping).await.expect("ping should be handled");
    owner
        .call(EagerAccess)
        .await
        .expect("eager access should be deferred safely");

    let exit = owner.shutdown(Shutdown::Stop).await;
    assert_eq!(exit.reason(), ExitReason::Stopped);
}

#[tokio::test]
async fn cx_address_borrow_survives_another_scope_borrow() {
    let owner = loac::spawn::<Worker>(());
    let actor = owner.actor_ref().clone();
    let (held_tx, held_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let holder = tokio::spawn(async move {
        actor
            .call(HoldAddress {
                held: held_tx,
                release: release_rx,
            })
            .await
    });

    held_rx.await.expect("holder should start");
    owner
        .call(OpenScope)
        .await
        .expect("scope borrow should complete");
    release_tx.send(()).expect("holder should remain alive");
    holder
        .await
        .expect("holder task should complete")
        .expect("holder reply should complete");

    let exit = owner.shutdown(Shutdown::Stop).await;
    assert_eq!(exit.reason(), ExitReason::Stopped);
}
