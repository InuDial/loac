//! Built-in reply concurrency profiles.
//!
//! [`#[actor(...)]`](macro@crate::actor) selects one profile:
//!
//! - omitting `mailbox` selects [`Disabled`];
//! - `mailbox` without `max_in_flight` selects [`Fixed`];
//! - fixed `max_in_flight` selects [`Fixed`];
//! - dynamic `max_in_flight` selects [`Dynamic`];
//! - unbounded `max_in_flight` selects [`Unbounded`].
//!
//! The macro reference documents syntax and defaults.
//! Manual [`crate::MessageConfig`] implementations select a profile directly.
//!
//! A finite limit bounds active replies.
//! At the limit, dispatch pauses before the next handler.
//! Every handler future runs on its actor task.
//! A [`Cx`](crate::Cx) reply may hold a scheduler lease.

mod profile;
mod queue;
mod runtime;
mod state;
mod wake;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use crate::{Actor, access::ScopedWake};

pub use profile::{Disabled, Dynamic, Fixed, SchedulerProfile, Unbounded};
pub(crate) use runtime::{ActorScheduler, RuntimeScheduler, SchedulerTurn, TurnContext};
pub(crate) use state::{
    DynamicLimit, FixedLimit, ReplyLane, ReplyProfile, ReplyState, UnboundedLimit,
};
pub(crate) use wake::{Lease, ReplySlot, ReplyWake};

pub(crate) struct ScheduledFuture {
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    wake: Arc<ReplyWake>,
    // One stable waker identity per reply.
    waker: Waker,
}

impl ScheduledFuture {
    /// Erases one actor-scoped future after establishing its runtime owner.
    ///
    /// The scheduler owns both the future and its wake state afterward.
    /// Keeping them together makes their cancellation order explicit.
    #[allow(unsafe_code)]
    pub(crate) fn scoped<'actor, A, F>(future: F, wake: ScopedWake<'actor, A>) -> Self
    where
        A: Actor,
        F: Future<Output = ()> + Send + 'actor,
    {
        let wake = wake.into_inner();
        let waker = wake.waker();
        let future: Pin<Box<dyn Future<Output = ()> + Send + 'actor>> = Box::pin(future);
        // SAFETY: actor-scoped futures enter only their actor's scheduler.
        // The actor task is their sole poller and cancellation owner.
        // RunningActor drops its scheduler before its pinned actor storage.
        // Cx exposes actor borrows only through higher-ranked closures.
        // Handler construction never receives a mutable actor reference.
        // Only one woken reply is polled at a time.
        let future = unsafe {
            std::mem::transmute::<
                Pin<Box<dyn Future<Output = ()> + Send + 'actor>>,
                Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
            >(future)
        };
        Self {
            future,
            wake,
            waker,
        }
    }

    /// Registers the actor task, then polls with the reply's stable waker.
    fn poll_with_task(&mut self, task: &mut Context<'_>) -> Poll<()> {
        self.register(task.waker());
        let mut item_task = Context::from_waker(&self.waker);
        self.future.as_mut().poll(&mut item_task)
    }

    fn take_ready(&self) -> bool {
        self.wake.take_ready()
    }

    fn is_ready(&self) -> bool {
        self.wake.is_ready()
    }

    fn register(&self, waker: &Waker) {
        self.wake.register(waker);
    }

    /// Wraps ordinary test work with queue-attached wake state.
    #[cfg(test)]
    pub(crate) fn test_with<F>(slot: ReplySlot, future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let wake = ReplyWake::new(slot);
        let waker = wake.waker();
        Self {
            future: Box::pin(future),
            wake,
            waker,
        }
    }
}
