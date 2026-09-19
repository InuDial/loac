mod support;

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use loac::{
    Actor, ActorRef, ActorScope, CallError, ChildExit, Cx, ExitReason, Handler, Message, Response,
    Shutdown, SpawnOptions, StreamHandler, StreamOut, actor, spawn_with,
};
use tokio::sync::{mpsc, oneshot};

use support::watchdog;

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|task| Poll::Ready(future.as_mut().poll(task))).await
}

#[derive(Message)]
#[message(reply = ())]
struct PendingReply {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct HookChild;

#[actor(mailbox)]
impl Actor for HookChild {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct StopChild;

impl Handler<StopChild> for HookChild {
    async fn handle(_message: StopChild, mut cx: Cx<'_, Self>) {
        cx.with(|_, scope| {
            let _ = scope.request_shutdown(Shutdown::Stop);
        });
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ExclusiveGate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[path = "replies/basic.rs"]
mod basic;
#[path = "replies/cancellation.rs"]
mod cancellation;
#[path = "replies/cx_exclusive.rs"]
mod cx_exclusive;
#[path = "replies/exclusive.rs"]
mod exclusive;
#[path = "replies/fairness.rs"]
mod fairness;
#[path = "replies/panic.rs"]
mod panic;
#[path = "replies/self_call.rs"]
mod self_call;
#[path = "replies/stop.rs"]
mod stop;
