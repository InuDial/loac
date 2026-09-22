#![allow(unsafe_code)]

//! Scoped actor access for handler futures.
//!
//! `Cx` is the unsafe capsule for [`Future`] handlers.
//! It exposes actor state only within synchronous scopes.
//! The runtime creates one handle per reply.
//! The actor task polls that reply.
//! No actor-aware operation overlaps another.
//!
//! Each reply owns one wake state.
//! A wake marks that reply ready and wakes the actor task.
//! The scheduler drains ready replies without polling the rest.
//! [`Cx::waker`] shares the same wake state with the handler.
//!
//! One actor-wide [`Lease`] backs [`Cx::exclusive`].
//! The holder records the exclusive reply in one atomic slot.

use std::{
    cell::Cell,
    marker::PhantomData,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Wake, Waker},
};

use crossbeam_queue::SegQueue;
use futures_util::task::AtomicWaker;
use slotmap::{DefaultKey, Key, KeyData};

use crate::{
    Actor, ActorRef, ActorScope,
    mailbox::Control,
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

/// Wake state for one scheduled reply.
///
/// The waker pushes the reply key once per ready episode.
/// The scheduler clears the flag before polling the reply.
/// Duplicate and stale keys are rejected during the drain.
/// Queue coordinates arrive before the wake exists.
pub(crate) struct ReplyWake {
    key: DefaultKey,
    ready: AtomicBool,
    queue: Arc<SegQueue<DefaultKey>>,
    lease: Arc<Lease>,
    task: AtomicWaker,
}

/// Queue coordinates handed to one reply builder.
///
/// The slot map allocates the key before the value exists.
/// A builder therefore never observes an unattached wake.
pub(crate) struct ReplySlot {
    pub(crate) key: DefaultKey,
    pub(crate) ready: Arc<SegQueue<DefaultKey>>,
    pub(crate) lease: Arc<Lease>,
}

/// The sole actor-wide exclusive lease.
///
/// The slot stores the held reply key, or `0` when free.
/// Debug builds compare and swap; release builds store.
/// The type also clears the slot when a holder leaves the queue.
pub(crate) struct Lease {
    holder: AtomicU64,
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

impl ReplyWake {
    pub(crate) fn new(slot: ReplySlot) -> Arc<Self> {
        Arc::new(Self {
            key: slot.key,
            ready: AtomicBool::new(false),
            queue: slot.ready,
            lease: slot.lease,
            task: AtomicWaker::new(),
        })
    }

    /// Marks one ready episode without waking the actor task.
    ///
    /// Dispatch uses this for the implicit first poll.
    pub(crate) fn mark_ready(&self) {
        if !self.ready.swap(true, Ordering::AcqRel) {
            self.queue.push(self.key);
        }
    }

    /// Marks this reply ready and wakes the actor task.
    pub(crate) fn notify(&self) {
        self.mark_ready();
        self.wake_task();
    }

    /// Wakes the actor task through the registered waker.
    ///
    /// A reactor may invoke a retained waker after this reply is gone.
    /// Panic containment keeps that wake from unwinding the reactor.
    pub(crate) fn wake_task(&self) {
        Control::contain_unwind(|| self.task.wake());
    }

    pub(crate) fn take_ready(&self) -> bool {
        self.ready.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.task.register(waker);
    }

    pub(crate) fn waker(self: &Arc<Self>) -> Waker {
        Waker::from(Arc::clone(self))
    }

    fn acquire_lease(&self) {
        self.lease.acquire(self.key);
    }

    fn release_lease(&self) {
        self.lease.release(self.key);
    }

    #[cfg(test)]
    pub(crate) fn acquire_lease_for_test(&self) {
        self.lease.acquire_for_test(self.key);
    }
}

impl Wake for ReplyWake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

impl Lease {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            holder: AtomicU64::new(0),
        })
    }

    /// Records one exclusive holder.
    ///
    /// Only the actor task reaches this path.
    /// While a lease is held, no other reply is polled.
    /// The guard's `&mut Cx` borrow forbids nested guards.
    fn acquire(&self, key: DefaultKey) {
        let ffi = key.data().as_ffi();
        #[cfg(debug_assertions)]
        self.holder
            .compare_exchange(0, ffi, Ordering::AcqRel, Ordering::Acquire)
            .expect("a Cx handler cannot hold nested exclusive guards");
        #[cfg(not(debug_assertions))]
        self.holder.store(ffi, Ordering::Release);
    }

    /// Clears the slot when it still holds `key`.
    fn release(&self, key: DefaultKey) {
        #[cfg(debug_assertions)]
        {
            let ffi = key.data().as_ffi();
            match self
                .holder
                .compare_exchange(ffi, 0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) | Err(0) => {}
                Err(holder) => panic!("lease holder mismatch: {holder:#x}"),
            }
        }
        #[cfg(not(debug_assertions))]
        {
            // The key only feeds the debug holder check.
            let _ = key;
            self.holder.store(0, Ordering::Release);
        }
    }

    /// Clears the slot after its holder leaves the queue.
    ///
    /// Completion, cancellation, and a forgotten guard all end here.
    pub(crate) fn force_release(&self) {
        self.holder.store(0, Ordering::Release);
    }

    /// Returns the exclusive reply key, if any.
    pub(crate) fn held(&self) -> Option<DefaultKey> {
        let ffi = self.holder.load(Ordering::Acquire);
        (ffi != 0).then(|| DefaultKey::from(KeyData::from_ffi(ffi)))
    }

    #[cfg(test)]
    pub(crate) fn acquire_for_test(&self, key: DefaultKey) {
        self.acquire(key);
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
    /// This is shorthand for waking [`Cx::waker`].
    /// External callers clone the waker instead.
    pub fn wake(&self) {
        self.reply.notify();
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
