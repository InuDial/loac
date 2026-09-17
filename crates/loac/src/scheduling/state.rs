use std::{marker::PhantomData, num::NonZeroUsize};

use crate::Actor;

use super::{Dynamic, Fixed, ScheduledFuture, Unbounded, queue::Queue};

pub(crate) struct InterleavedState<A: Actor, L> {
    pub(super) queue: Queue,
    pub(super) limit: L,
    pub(crate) cursor: InterleavedLane,
    actor: PhantomData<fn() -> A>,
}

pub(crate) trait InterleavedProfile<A: Actor>: Send + 'static {
    type Limit: LimitPolicy;

    fn state(&mut self) -> &mut InterleavedState<A, Self::Limit>;
}

impl<A: Actor, const N: usize> InterleavedProfile<A> for Fixed<A, N> {
    type Limit = FixedLimit<N>;

    fn state(&mut self) -> &mut InterleavedState<A, Self::Limit> {
        &mut self.state
    }
}

impl<A: Actor> InterleavedProfile<A> for Dynamic<A> {
    type Limit = DynamicLimit;

    fn state(&mut self) -> &mut InterleavedState<A, Self::Limit> {
        &mut self.state
    }
}

impl<A: Actor> InterleavedProfile<A> for Unbounded<A> {
    type Limit = UnboundedLimit;

    fn state(&mut self) -> &mut InterleavedState<A, Self::Limit> {
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

impl<A: Actor, L: LimitPolicy> InterleavedState<A, L> {
    pub(super) fn with_limit(limit: L) -> Self {
        Self {
            queue: Queue::new(),
            limit,
            cursor: InterleavedLane::Mailbox,
            actor: PhantomData,
        }
    }

    pub(crate) fn has_dispatch_capacity(&self) -> bool {
        !self.queue.is_leased() && self.limit.has_capacity(self.queue.len())
    }

    pub(crate) fn has_interleaved(&self) -> bool {
        !self.queue.is_empty()
    }

    pub(super) fn push_interleaved(&mut self, future: ScheduledFuture) {
        debug_assert!(self.has_dispatch_capacity());
        self.queue.push(future);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SerialLane {
    Mailbox,
    ChildExit,
}

impl SerialLane {
    pub(super) const fn next(self) -> Self {
        match self {
            Self::Mailbox => Self::ChildExit,
            Self::ChildExit => Self::Mailbox,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InterleavedLane {
    Mailbox,
    Interleaved,
    ChildExit,
}

impl InterleavedLane {
    pub(super) const fn next(self) -> Self {
        match self {
            Self::Mailbox => Self::Interleaved,
            Self::Interleaved => Self::ChildExit,
            Self::ChildExit => Self::Mailbox,
        }
    }
}
