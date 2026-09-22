use std::{marker::PhantomData, num::NonZeroUsize};

use crate::Actor;

use super::{Dynamic, Fixed, ScheduledFuture, Unbounded, queue::Queue};

pub(crate) struct ReplyState<A: Actor, L> {
    pub(super) queue: Queue,
    pub(super) limit: L,
    pub(crate) cursor: ReplyLane,
    actor: PhantomData<fn() -> A>,
}

pub(crate) trait ReplyProfile<A: Actor>: Send + 'static {
    type Limit: LimitPolicy;

    fn state(&mut self) -> &mut ReplyState<A, Self::Limit>;
}

impl<A: Actor, const N: usize> ReplyProfile<A> for Fixed<A, N> {
    type Limit = FixedLimit<N>;

    fn state(&mut self) -> &mut ReplyState<A, Self::Limit> {
        &mut self.state
    }
}

impl<A: Actor> ReplyProfile<A> for Dynamic<A> {
    type Limit = DynamicLimit;

    fn state(&mut self) -> &mut ReplyState<A, Self::Limit> {
        &mut self.state
    }
}

impl<A: Actor> ReplyProfile<A> for Unbounded<A> {
    type Limit = UnboundedLimit;

    fn state(&mut self) -> &mut ReplyState<A, Self::Limit> {
        &mut self.state
    }
}

pub(crate) struct FixedLimit<const N: usize>;

pub(crate) struct DynamicLimit(pub(super) NonZeroUsize);

pub(crate) struct UnboundedLimit;

pub(crate) trait LimitPolicy: Send + 'static {
    fn has_capacity(&self, active: usize) -> bool;
}

impl<const N: usize> LimitPolicy for FixedLimit<N> {
    fn has_capacity(&self, active: usize) -> bool {
        active < N
    }
}

impl LimitPolicy for DynamicLimit {
    fn has_capacity(&self, active: usize) -> bool {
        active < self.0.get()
    }
}

impl LimitPolicy for UnboundedLimit {
    fn has_capacity(&self, _active: usize) -> bool {
        true
    }
}

impl<A: Actor, L: LimitPolicy> ReplyState<A, L> {
    pub(super) fn with_limit(limit: L) -> Self {
        Self {
            queue: Queue::new(),
            limit,
            cursor: ReplyLane::Mailbox,
            actor: PhantomData,
        }
    }

    pub(crate) fn has_dispatch_capacity(&self) -> bool {
        !self.queue.is_leased() && self.limit.has_capacity(self.queue.len())
    }

    pub(crate) fn has_replies(&self) -> bool {
        !self.queue.is_empty()
    }

    pub(crate) fn push(&mut self, future: ScheduledFuture) {
        debug_assert!(self.has_dispatch_capacity());
        let _ = self.queue.push(future);
    }

    #[cfg(test)]
    pub(crate) fn push_leased(&mut self, future: ScheduledFuture) {
        self.queue.push_leased(future);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplyLane {
    Mailbox,
    Reply,
    ChildExit,
}

impl ReplyLane {
    pub(super) const fn next(self) -> Self {
        match self {
            Self::Mailbox => Self::Reply,
            Self::Reply => Self::ChildExit,
            Self::ChildExit => Self::Mailbox,
        }
    }
}
