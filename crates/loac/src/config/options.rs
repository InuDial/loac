use std::{fmt, marker::PhantomData, num::NonZeroUsize};

/// Default capacity used by built-in mailbox configurations.
#[doc(hidden)]
pub const DEFAULT_MAILBOX_CAPACITY: usize = 32;

/// Default limit used by built-in concurrency configurations.
#[doc(hidden)]
pub const DEFAULT_MAX_IN_FLIGHT: usize = 32;

/// Default limit used by built-in child configurations.
#[doc(hidden)]
pub const DEFAULT_MAX_CHILDREN: usize = 32;

/// Built-in spawn options for one actor.
///
/// `M` retains only mailbox values which may vary per spawn.
/// `F` retains only handler limits which may vary per spawn.
/// `C` retains only child limits which may vary per spawn.
/// The actor marker prevents options from crossing actor types.
pub struct ActorOptions<A, M, F, C> {
    mailbox: M,
    max_in_flight: F,
    children: C,
    actor: PhantomData<fn() -> A>,
}

impl<A, M, C, const DEFAULT: usize> ActorOptions<A, M, DynamicMaxInFlight<DEFAULT>, C> {
    /// Sets the maximum number of active handler futures.
    #[must_use]
    pub const fn with_max_in_flight(mut self, max_in_flight: NonZeroUsize) -> Self {
        self.max_in_flight.value = Some(max_in_flight);
        self
    }

    /// Resolves this spawn's dynamic handler limit.
    #[doc(hidden)]
    pub const fn max_in_flight(&self) -> NonZeroUsize {
        match self.max_in_flight.value {
            Some(max_in_flight) => max_in_flight,
            None => DynamicMaxInFlight::<DEFAULT>::DEFAULT_MAX_IN_FLIGHT,
        }
    }
}

impl<A, F, C, const DEFAULT: usize> ActorOptions<A, DynamicMailboxCapacity<DEFAULT>, F, C> {
    /// Overrides this actor's dynamic mailbox capacity.
    #[must_use]
    pub const fn with_mailbox_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.mailbox.capacity = Some(capacity);
        self
    }

    /// Resolves this spawn's dynamic mailbox capacity.
    #[doc(hidden)]
    pub const fn mailbox_capacity(&self) -> NonZeroUsize {
        match self.mailbox.capacity {
            Some(capacity) => capacity,
            None => DynamicMailboxCapacity::<DEFAULT>::DEFAULT_CAPACITY,
        }
    }
}

impl<A, M, F, const DEFAULT: usize> ActorOptions<A, M, F, DynamicMaxChildren<DEFAULT>> {
    /// Overrides this actor's direct-child limit.
    #[must_use]
    pub const fn with_max_children(mut self, max_children: NonZeroUsize) -> Self {
        self.children.max_children = Some(max_children);
        self
    }

    /// Resolves this spawn's direct-child limit.
    #[doc(hidden)]
    pub const fn max_children(&self) -> NonZeroUsize {
        match self.children.max_children {
            Some(max_children) => max_children,
            None => DynamicMaxChildren::<DEFAULT>::DEFAULT_MAX_CHILDREN,
        }
    }
}

impl<A, M: Clone, F: Clone, C: Clone> Clone for ActorOptions<A, M, F, C> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox.clone(),
            max_in_flight: self.max_in_flight.clone(),
            children: self.children.clone(),
            actor: PhantomData,
        }
    }
}

impl<A, M: Copy, F: Copy, C: Copy> Copy for ActorOptions<A, M, F, C> {}

impl<A, M: fmt::Debug, F: fmt::Debug, C: fmt::Debug> fmt::Debug for ActorOptions<A, M, F, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorOptions")
            .field("mailbox", &self.mailbox)
            .field("max_in_flight", &self.max_in_flight)
            .field("children", &self.children)
            .finish()
    }
}

impl<A, M: PartialEq, F: PartialEq, C: PartialEq> PartialEq for ActorOptions<A, M, F, C> {
    fn eq(&self, other: &Self) -> bool {
        self.mailbox == other.mailbox
            && self.max_in_flight == other.max_in_flight
            && self.children == other.children
    }
}

impl<A, M: Eq, F: Eq, C: Eq> Eq for ActorOptions<A, M, F, C> {}

impl<A, M: Default, F: Default, C: Default> Default for ActorOptions<A, M, F, C> {
    fn default() -> Self {
        Self {
            mailbox: M::default(),
            max_in_flight: F::default(),
            children: C::default(),
            actor: PhantomData,
        }
    }
}

/// Spawn state for an actor without a mailbox.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoMailbox;

/// Spawn state for an actor with a fixed mailbox.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixedMailboxCapacity;

/// Spawn state for an actor with an unbounded mailbox.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnboundedMailboxCapacity;

/// Spawn state without a handler scheduler.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoMaxInFlight;

/// Spawn state with a fixed handler-concurrency limit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixedMaxInFlight;

/// Spawn state with unbounded handler concurrency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnboundedMaxInFlight;

/// Spawn state for an actor without direct children.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoChildren;

/// Spawn state for an actor with a fixed child limit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixedMaxChildren;

/// Spawn state for an actor with unbounded direct children.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnboundedMaxChildren;

/// Spawn state for an actor with a dynamic mailbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicMailboxCapacity<const DEFAULT: usize = DEFAULT_MAILBOX_CAPACITY> {
    capacity: Option<NonZeroUsize>,
}

impl<const DEFAULT: usize> DynamicMailboxCapacity<DEFAULT> {
    const DEFAULT_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(DEFAULT).expect("mailbox capacity must be greater than zero");
}

impl<const DEFAULT: usize> Default for DynamicMailboxCapacity<DEFAULT> {
    fn default() -> Self {
        Self { capacity: None }
    }
}

/// Spawn state with dynamic handler concurrency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicMaxInFlight<const DEFAULT: usize = DEFAULT_MAX_IN_FLIGHT> {
    value: Option<NonZeroUsize>,
}

impl<const DEFAULT: usize> DynamicMaxInFlight<DEFAULT> {
    const DEFAULT_MAX_IN_FLIGHT: NonZeroUsize =
        NonZeroUsize::new(DEFAULT).expect("handler concurrency must be greater than zero");
}

impl<const DEFAULT: usize> Default for DynamicMaxInFlight<DEFAULT> {
    fn default() -> Self {
        Self { value: None }
    }
}

/// Spawn state for an actor with a dynamic child limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicMaxChildren<const DEFAULT: usize = DEFAULT_MAX_CHILDREN> {
    max_children: Option<NonZeroUsize>,
}

impl<const DEFAULT: usize> DynamicMaxChildren<DEFAULT> {
    const DEFAULT_MAX_CHILDREN: NonZeroUsize =
        NonZeroUsize::new(DEFAULT).expect("child limit must be greater than zero");
}

impl<const DEFAULT: usize> Default for DynamicMaxChildren<DEFAULT> {
    fn default() -> Self {
        Self { max_children: None }
    }
}

/// Options whose mailbox capacity may change per spawn.
pub trait DynamicMailboxCapacityOptions: Sized {
    /// Overrides the mailbox capacity for one spawn.
    #[must_use]
    fn with_mailbox_capacity(self, capacity: NonZeroUsize) -> Self;
}

impl<A, F, C, const DEFAULT: usize> DynamicMailboxCapacityOptions
    for ActorOptions<A, DynamicMailboxCapacity<DEFAULT>, F, C>
{
    fn with_mailbox_capacity(self, capacity: NonZeroUsize) -> Self {
        ActorOptions::with_mailbox_capacity(self, capacity)
    }
}

/// Options whose handler-concurrency limit may change per spawn.
pub trait DynamicMaxInFlightOptions: Sized {
    /// Overrides the active handler limit for one spawn.
    #[must_use]
    fn with_max_in_flight(self, max_in_flight: NonZeroUsize) -> Self;
}

impl<A, M, C, const DEFAULT: usize> DynamicMaxInFlightOptions
    for ActorOptions<A, M, DynamicMaxInFlight<DEFAULT>, C>
{
    fn with_max_in_flight(self, max_in_flight: NonZeroUsize) -> Self {
        ActorOptions::with_max_in_flight(self, max_in_flight)
    }
}

/// Options whose direct-child limit may change per spawn.
pub trait DynamicMaxChildrenOptions: Sized {
    /// Overrides the direct-child limit for one spawn.
    #[must_use]
    fn with_max_children(self, max_children: NonZeroUsize) -> Self;
}

impl<A, M, F, const DEFAULT: usize> DynamicMaxChildrenOptions
    for ActorOptions<A, M, F, DynamicMaxChildren<DEFAULT>>
{
    fn with_max_children(self, max_children: NonZeroUsize) -> Self {
        ActorOptions::with_max_children(self, max_children)
    }
}
