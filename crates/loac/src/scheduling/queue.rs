use std::{
    panic::{self, AssertUnwindSafe},
    sync::Arc,
    task::Context,
};

use crossbeam_queue::SegQueue;
use slotmap::{DefaultKey, SlotMap};

use crate::{
    access::Lease,
    mailbox::{Control, Mode},
};

use super::{ScheduledFuture, drop_without_unwind};

const ACTIVE_POLL_BUDGET: usize = 16;

/// Outcome of one reply collection poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReplyPoll {
    /// No reply was ready.
    Pending,
    /// A reply completed or lifecycle changed.
    Progress,
    /// One reply holds the scheduler lease.
    Leased,
    /// The budget ended with ready keys remaining.
    ///
    /// The caller must stop polling and return [`Poll::Pending`].
    BudgetExhausted,
}

/// Ready-key queue for one actor scheduler.
///
/// Items live in a slot map under stable versioned keys.
/// A wake pushes one key per ready episode.
/// The drain polls exactly the pushed keys.
/// The lease slot pauses every other reply while held.
pub(super) struct Queue {
    items: SlotMap<DefaultKey, ScheduledFuture>,
    ready: Arc<SegQueue<DefaultKey>>,
    lease: Arc<Lease>,
}

impl Queue {
    pub(super) fn new() -> Self {
        Self {
            items: SlotMap::new(),
            ready: Arc::new(SegQueue::new()),
            lease: Lease::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(super) fn is_leased(&self) -> bool {
        self.lease.held().is_some()
    }

    /// Inserts one reply and marks its first poll ready.
    ///
    /// Attachment happens before the first poll.
    /// No waker can escape before the entry exists.
    pub(super) fn push(&mut self, future: ScheduledFuture) -> DefaultKey {
        let key = self.items.insert(future);
        let item = &self.items[key];
        item.wake
            .attach(key, Arc::clone(&self.ready), Arc::clone(&self.lease));
        item.wake.mark_ready();
        key
    }

    #[cfg(test)]
    pub(super) fn push_leased(&mut self, future: ScheduledFuture) {
        let key = self.push(future);
        self.lease.acquire_for_test(key);
    }

    pub(super) fn poll(
        &mut self,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> ReplyPoll {
        match self.lease.held() {
            Some(key) => self.poll_leased(key, control, expected_mode, task),
            None => self.poll_ready(control, expected_mode, task),
        }
    }

    // While one reply is leased, no other reply runs.
    // Unrelated ready keys stay queued until release.
    fn poll_leased(
        &mut self,
        key: DefaultKey,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> ReplyPoll {
        let Some(item) = self.items.get_mut(key) else {
            self.lease.force_release();
            return ReplyPoll::Progress;
        };
        if !item.take_ready() {
            return ReplyPoll::Pending;
        }

        item.register(task.waker());
        let waker = item.waker();
        let mut item_task = Context::from_waker(&waker);
        let result = item.poll(&mut item_task);
        let held = self.lease.held() == Some(key);

        if control.mode() != expected_mode {
            return ReplyPoll::Progress;
        }
        if result.is_ready() {
            self.complete(key, control);
            return ReplyPoll::Progress;
        }
        if held {
            return ReplyPoll::Leased;
        }
        ReplyPoll::Progress
    }

    // One drain polls at most ACTIVE_POLL_BUDGET replies.
    // A truncated drain self-wakes and reports BudgetExhausted.
    // Ready keys are cleared before their poll.
    // A wake during that poll pushes a fresh key.
    fn poll_ready(
        &mut self,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> ReplyPoll {
        let mut polled = 0;
        let mut completed = false;

        while polled < ACTIVE_POLL_BUDGET {
            if control.mode() != expected_mode {
                break;
            }
            let Some(key) = self.ready.pop() else {
                break;
            };
            let Some(item) = self.items.get_mut(key) else {
                continue;
            };
            if !item.take_ready() {
                continue;
            }

            item.register(task.waker());
            let waker = item.waker();
            let mut item_task = Context::from_waker(&waker);
            let result = item.poll(&mut item_task);
            let leased = self.lease.held().is_some();

            if result.is_ready() {
                self.complete(key, control);
                completed = true;
            } else if leased {
                return ReplyPoll::Leased;
            }
            polled += 1;
        }

        if control.mode() != expected_mode {
            return ReplyPoll::Progress;
        }
        if !self.ready.is_empty() {
            // A continuation wake must not unwind the actor task.
            if let Err(payload) =
                panic::catch_unwind(AssertUnwindSafe(|| task.waker().wake_by_ref()))
            {
                Control::discard_panic(payload);
            }
            return ReplyPoll::BudgetExhausted;
        }
        if completed {
            ReplyPoll::Progress
        } else {
            ReplyPoll::Pending
        }
    }

    /// Removes one reply and releases any forgotten lease.
    ///
    /// The lease normally clears when its guard drops.
    /// A leaked guard would otherwise pause the actor forever.
    fn complete(&mut self, key: DefaultKey, control: &Control) {
        if let Some(future) = self.items.remove(key) {
            control.drop_user_value(future);
        }
        if self.lease.held() == Some(key) {
            self.lease.force_release();
        }
    }

    pub(super) fn clear(&mut self, control: &Control) {
        for (_, future) in self.items.drain() {
            control.drop_user_value(future);
        }
        self.lease.force_release();
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        for (_, future) in self.items.drain() {
            drop_without_unwind(future);
        }
        self.lease.force_release();
    }
}

#[cfg(test)]
mod tests;
