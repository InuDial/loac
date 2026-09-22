#![allow(unsafe_code)]

//! Scoped actor access for handler futures.
//!
//! `Cx` is the unsafe capsule for [`Future`] handlers.
//! It exposes actor state only within synchronous scopes.
//! The runtime creates one handle per reply.
//! The actor task polls that reply.
//! No actor-aware operation overlaps another.
//!
//! Each reply owns one wake state from [`scheduling`](crate::scheduling).
//! A wake marks that reply ready and wakes the actor task.
//! The scheduler drains ready replies without polling the rest.
//! [`Cx::waker`] shares the same wake state with the handler.
//!
//! One actor-wide lease backs [`Cx::exclusive`].
//! The holder records the exclusive reply in one atomic slot.

use std::{cell::Cell, marker::PhantomData, ops::Deref, sync::Arc, task::Waker};

use crate::{
    Actor, ActorRef, ActorScope,
    runtime::{ActorAccess, CxTarget},
    scheduling::{ReplySlot, ReplyWake},
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
    reply: Arc<ReplyWake>,
    _lifetime: PhantomData<&'a mut A>,
    // Cx may move with its actor task.
    // It cannot be shared across threads.
    _not_sync: PhantomData<Cell<()>>,
}

/// Proves that one reply wake belongs to live actor storage.
pub(crate) struct ScopedWake<'actor, A: Actor> {
    wake: Arc<ReplyWake>,
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

impl<A: Actor> ScopedWake<'_, A> {
    /// Consumes the witness after scheduler ownership becomes established.
    pub(crate) fn into_inner(self) -> Arc<ReplyWake> {
        self.wake
    }
}

impl<'actor, A: Actor> Cx<'actor, A> {
    /// Creates paired access and liveness proof for one dispatch.
    ///
    /// This operation never borrows the stored actor.
    pub(crate) fn new(
        owner: &'actor mut ActorAccess<A>,
        slot: ReplySlot,
    ) -> (Self, ScopedWake<'actor, A>) {
        let reply = ReplyWake::new(slot);
        let scoped_wake = ScopedWake {
            wake: Arc::clone(&reply),
            _lifetime: PhantomData,
        };
        let cx = Cx {
            target: owner.target(),
            reply,
            _lifetime: PhantomData,
            _not_sync: PhantomData,
        };
        (cx, scoped_wake)
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

    /// Returns a waker for this handler future.
    ///
    /// The waker is `Send + Sync` and stays valid after completion.
    /// Later wakes become no-ops once the reply leaves the queue.
    /// Waking schedules one poll of this handler alone.
    #[must_use]
    pub fn waker(&self) -> Waker {
        self.reply.waker()
    }

    /// Schedules this handler future for another poll.
    ///
    /// `Cx` runs on the actor task, so the current drain or its budget
    /// continuation observes the ready key. External wake sources clone
    /// [`Cx::waker`] instead.
    pub fn wake(&self) {
        self.reply.mark_ready();
    }

    /// Pauses scheduled actor work until the returned guard drops.
    ///
    /// Acquisition is immediate during the current handler poll.
    /// A retained guard pauses every other scheduled reply.
    /// Graceful [`Actor::on_shutdown`] may preempt this lease.
    /// Use [`ExclusiveGuard::with`] for temporary state access.
    pub fn exclusive<'cx>(&'cx mut self) -> ExclusiveGuard<'cx, 'actor, A> {
        self.reply.acquire_lease();
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
        // Release is always observed in the same poll.
        // Waking here would consume the registered task waker.
        self.cx.reply.release_lease();
    }
}

// SAFETY: only the actor task polls the owning future.
// Every mutation occurs during one serialized poll.
unsafe impl<A: Actor> Send for Cx<'_, A> {}
