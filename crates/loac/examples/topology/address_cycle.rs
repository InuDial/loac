//! Builds a parent-child address cycle during scope-based initialization.
//! The parent runtime owns the child; both actors keep non-owning addresses.

use loac::{ActorRef, prelude::*};
use tokio::sync::oneshot;

struct Parent {
    child: ActorRef<Child>,
}

#[actor(mailbox, children, max_children = unbounded)]
impl Actor for Parent {
    type SpawnArgs = ();

    async fn init(_: Self::SpawnArgs, scope: &mut ActorScope<'_, Self>) -> Self {
        // The address exists before Parent. The child can retain it immediately.
        let parent = scope.clone();
        let Ok(child) = scope.spawn_child::<Child>(parent);
        let child = child.into_actor_ref();

        Self { child }
    }
}

struct Child {
    parent: ActorRef<Parent>,
}

#[actor(mailbox)]
impl Actor for Child {
    type SpawnArgs = ActorRef<Parent>;

    async fn init(parent: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self { parent }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Start {
    completed: oneshot::Sender<()>,
}

#[derive(Message)]
#[message(reply = ())]
struct VisitChild {
    completed: oneshot::Sender<()>,
}

#[derive(Message)]
#[message(reply = ())]
struct ReturnToParent {
    completed: oneshot::Sender<()>,
}

impl Handler<Start> for Parent {
    async fn handle(message: Start, mut cx: Cx<'_, Self>) {
        cx.with(|actor, _| {
            assert!(
                actor
                    .child
                    .try_send(VisitChild {
                        completed: message.completed,
                    })
                    .is_ok()
            );
        });
    }
}

impl Handler<VisitChild> for Child {
    async fn handle(message: VisitChild, mut cx: Cx<'_, Self>) {
        cx.with(|actor, _| {
            assert!(
                actor
                    .parent
                    .try_send(ReturnToParent {
                        completed: message.completed,
                    })
                    .is_ok()
            );
        });
    }
}

impl Handler<ReturnToParent> for Parent {
    async fn handle(message: ReturnToParent, _cx: Cx<'_, Self>) {
        let _ = message.completed.send(());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (completed_tx, completed_rx) = oneshot::channel();
    let owner = loac::spawn::<Parent>(());

    owner
        .send(Start {
            completed: completed_tx,
        })
        .await?;
    completed_rx.await?;

    let status = owner.shutdown(loac::Shutdown::Drain).await;
    assert_eq!(status.reason(), loac::ExitReason::Drained);
    assert_eq!(status.subtree(), loac::SubtreeStatus::Terminated);
    Ok(())
}
