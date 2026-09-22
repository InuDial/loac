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

use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use crate::{
    Actor,
    access::{ReplyWake, ScopedWake},
    mailbox::Control,
};

pub use profile::{Disabled, Dynamic, Fixed, SchedulerProfile, Unbounded};
pub(crate) use runtime::{ActorScheduler, RuntimeScheduler, SchedulerTurn, TurnContext};
pub(crate) use state::{
    DynamicLimit, FixedLimit, ReplyLane, ReplyProfile, ReplyState, UnboundedLimit,
};

pub(crate) struct ScheduledFuture {
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    wake: Arc<ReplyWake>,
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
        Self { future, wake }
    }

    fn poll(&mut self, task: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(task)
    }

    fn take_ready(&self) -> bool {
        self.wake.take_ready()
    }

    fn register(&self, waker: &Waker) {
        self.wake.register(waker);
    }

    fn waker(&self) -> Waker {
        self.wake.waker()
    }

    /// Wraps ordinary test work with an unattached wake state.
    #[cfg(test)]
    pub(crate) fn test<F>(future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
            wake: ReplyWake::new(),
        }
    }
}

// Automatic frame destruction cannot borrow the actor's lifecycle control.
// Isolate each value so one Drop panic cannot skip sibling cleanup.
fn drop_without_unwind<T>(value: T) {
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(value))) {
        Control::discard_panic(payload);
    }
}
