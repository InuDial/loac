//! Built-in reply concurrency profiles.
//!
//! [`#[actor(...)]`](macro@crate::actor) selects one profile:
//!
//! - omitting `mailbox` selects [`Disabled`];
//! - `mailbox` without `interleaved` selects [`Serial`];
//! - fixed `interleaved` forms select [`Fixed`];
//! - dynamic `interleaved` forms select [`Dynamic`];
//! - unbounded `interleaved` selects [`Unbounded`].
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
    task::{Context, Poll},
};

use crate::{
    Actor,
    access::{ReplyLease, ScopedLease},
    mailbox::Control,
};

pub use profile::{Disabled, Dynamic, Fixed, SchedulerProfile, Serial, Unbounded};
pub(crate) use runtime::{ActorScheduler, RuntimeScheduler, SchedulerTurn, TurnContext};
pub(crate) use state::{
    DynamicLimit, FixedLimit, ReplyLane, ReplyProfile, ReplyState, UnboundedLimit,
};

pub(crate) struct ScheduledFuture {
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    lease: Arc<ReplyLease>,
}

impl ScheduledFuture {
    /// Erases one actor-scoped future after establishing its runtime owner.
    ///
    /// The scheduler owns both the future and its lease afterward.
    /// Keeping them together makes their cancellation order explicit.
    #[allow(unsafe_code)]
    pub(crate) fn scoped<'actor, A, F>(future: F, lease: ScopedLease<'actor, A>) -> Self
    where
        A: Actor,
        F: Future<Output = ()> + Send + 'actor,
    {
        let lease = lease.into_inner();
        let future: Pin<Box<dyn Future<Output = ()> + Send + 'actor>> = Box::pin(future);
        // SAFETY: actor-scoped futures enter only their actor's scheduler.
        // The actor task is their sole poller and cancellation owner.
        // RunningActor drops its scheduler before its pinned actor storage.
        // Cx exposes actor borrows only through higher-ranked closures.
        // Handler construction never receives a mutable actor reference.
        // The paired lease serializes every actor-aware future poll.
        let future = unsafe {
            std::mem::transmute::<
                Pin<Box<dyn Future<Output = ()> + Send + 'actor>>,
                Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
            >(future)
        };
        Self { future, lease }
    }

    fn poll(&mut self, task: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(task)
    }

    fn is_leased(&self) -> bool {
        self.lease.is_held()
    }

    /// Wraps ordinary test work with an inactive lease.
    #[cfg(test)]
    pub(crate) fn test<F>(future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
            lease: ReplyLease::new(),
        }
    }

    /// Wraps test work with a lease acquired after queue insertion.
    #[cfg(test)]
    pub(crate) fn test_scoped<F>(future: F) -> (Self, Arc<ReplyLease>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let lease = ReplyLease::new();
        let scheduled = Self {
            future: Box::pin(future),
            lease: Arc::clone(&lease),
        };
        (scheduled, lease)
    }
}

// Automatic frame destruction cannot borrow the actor's lifecycle control.
// Isolate each value so one Drop panic cannot skip sibling cleanup.
fn drop_without_unwind<T>(value: T) {
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(value))) {
        Control::discard_panic(payload);
    }
}
