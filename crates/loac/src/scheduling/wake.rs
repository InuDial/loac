//! Per-reply wake state and the actor-wide exclusive lease.
//!
//! [`ReplySlot`] carries the queue coordinates assigned at insertion.
//! [`ReplyWake`] pushes one ready key per ready episode.
//! The scheduler drains pushed keys without polling the rest.
//! [`Lease`] stores the exclusive reply key in one atomic slot.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Wake, Waker},
};

use crossbeam_queue::SegQueue;
use futures_util::task::AtomicWaker;
use slotmap::{DefaultKey, Key, KeyData};

use crate::mailbox::Control;

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
    fn wake_task(&self) {
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

    pub(crate) fn acquire_lease(&self) {
        self.lease.acquire(self.key);
    }

    pub(crate) fn release_lease(&self) {
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
