#![allow(unsafe_code)]

//! Scoped actor access for handler futures.
//!
//! `Cx` is the unsafe capsule for [`Future`] handlers.
//! It exposes actor state only within synchronous scopes.
//! The runtime creates one handle per reply.
//! The actor task polls that reply.
//! No actor-aware operation overlaps another.

use std::{
    cell::Cell,
    marker::PhantomData,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    Actor, ActorRef, ActorScope,
    runtime::{ActorAccess, CxTarget},
};

/// Owned access handle for actor-aware handler futures.
///
/// [`Handler`](crate::Handler) receives this handle.
/// [`StreamHandler`](crate::StreamHandler) does likewise.
/// The runtime polls each handler on its actor task.
/// Safe code cannot erase the handle's dispatch lifetime.
/// `Cx` dereferences to [`ActorRef`] for address operations.
pub struct Cx<'a, A: Actor + 'a> {
    target: CxTarget<A>,
    lease: Arc<ReplyLease>,
    _lifetime: PhantomData<&'a mut A>,
    // Cx may move with its actor task.
    // It cannot be shared across threads.
    _not_sync: PhantomData<Cell<()>>,
}

/// Proves that one lease belongs to live actor storage.
pub(crate) struct ScopedLease<'actor, A: Actor> {
    lease: Arc<ReplyLease>,
    _lifetime: PhantomData<&'actor A>,
}

/// Temporarily pauses scheduled actor work across await points.
///
/// The guard borrows its originating [`Cx`].
/// It stores no actor or scope references.
/// [`with`](Self::with) creates temporary actor access.
/// Graceful [`Actor::on_shutdown`] may still run during the lease.
#[must_use = "dropping the guard releases exclusive scheduling"]
pub struct ExclusiveGuard<'cx, 'actor, A: Actor> {
    cx: &'cx mut Cx<'actor, A>,
}

impl<A: Actor> ExclusiveGuard<'_, '_, A> {
    /// Runs `f` with temporary actor and scope borrows.
    pub fn with<R>(
        &mut self,
        f: impl for<'a> FnOnce(&'a mut A, &'a mut ActorScope<'a, A>) -> R,
    ) -> R {
        self.cx.with(f)
    }

    /// Returns this actor's non-owning address.
    #[must_use]
    pub fn myself(&self) -> &ActorRef<A> {
        self.cx.myself()
    }
}

pub(crate) struct ReplyLease {
    held: AtomicBool,
}

impl ReplyLease {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            held: AtomicBool::new(false),
        })
    }

    pub(crate) fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }

    #[cfg(test)]
    /// Acquires a queued lease without constructing a public guard.
    pub(crate) fn acquire_for_test(&self) {
        self.acquire();
    }

    fn acquire(&self) {
        assert!(
            !self.held.swap(true, Ordering::AcqRel),
            "a Cx handler cannot hold nested exclusive guards"
        );
    }

    fn release(&self) {
        self.held.store(false, Ordering::Release);
    }
}

impl<A: Actor> ScopedLease<'_, A> {
    /// Consumes the witness after scheduler ownership becomes established.
    pub(crate) fn into_inner(self) -> Arc<ReplyLease> {
        self.lease
    }
}

impl<'actor, A: Actor> Cx<'actor, A> {
    /// Creates paired access and liveness proof for one dispatch.
    ///
    /// This operation never borrows the stored actor.
    pub(crate) fn new(owner: &'actor mut ActorAccess<A>) -> (Self, ScopedLease<'actor, A>) {
        let lease = ReplyLease::new();
        let scoped_lease = ScopedLease {
            lease: Arc::clone(&lease),
            _lifetime: PhantomData,
        };
        let cx = Cx {
            target: owner.target(),
            lease,
            _lifetime: PhantomData,
            _not_sync: PhantomData,
        };
        (cx, scoped_lease)
    }

    /// Runs `f` with temporary `&mut A` and [`ActorScope`] borrows.
    ///
    /// Use `_` for any unused borrow.
    /// The closure prevents either borrow from escaping.
    pub fn with<R>(
        &mut self,
        f: impl for<'a> FnOnce(&'a mut A, &'a mut ActorScope<'a, A>) -> R,
    ) -> R {
        // SAFETY: actor-aware polls never overlap.
        // The runtime retains the pinned target cell.
        unsafe { self.target.with(f) }
    }

    /// Returns this actor's non-owning address.
    ///
    /// This opens no actor or scope borrow.
    #[must_use]
    pub fn myself(&self) -> &ActorRef<A> {
        // SAFETY: the runtime retains the pinned target cell.
        unsafe { self.target.actor_ref() }
    }

    /// Pauses scheduled actor work until the returned guard drops.
    ///
    /// Acquisition is immediate during the current handler poll.
    /// A retained guard pins this scheduler item.
    /// Graceful [`Actor::on_shutdown`] may preempt this lease.
    /// Use [`ExclusiveGuard::with`] for temporary state access.
    pub fn exclusive<'cx>(&'cx mut self) -> ExclusiveGuard<'cx, 'actor, A> {
        self.lease.acquire();
        ExclusiveGuard { cx: self }
    }
}

impl<A: Actor> Deref for Cx<'_, A> {
    type Target = ActorRef<A>;

    fn deref(&self) -> &Self::Target {
        self.myself()
    }
}

impl<A: Actor> Drop for ExclusiveGuard<'_, '_, A> {
    fn drop(&mut self) {
        self.cx.lease.release();
    }
}

// SAFETY: only the actor task polls the owning future.
// Every mutation occurs during one serialized poll.
unsafe impl<A: Actor> Send for Cx<'_, A> {}
