mod options;

#[cfg(test)]
mod tests;

pub use options::{
    ActorOptions, DEFAULT_MAILBOX_CAPACITY, DEFAULT_MAX_CHILDREN, DEFAULT_MAX_IN_FLIGHT,
    DynamicMailboxCapacity, DynamicMailboxCapacityOptions, DynamicMaxChildren,
    DynamicMaxChildrenOptions, DynamicMaxInFlight, DynamicMaxInFlightOptions, FixedMailboxCapacity,
    FixedMaxChildren, FixedMaxInFlight, NoChildren, NoMailbox, NoMaxInFlight,
    UnboundedMailboxCapacity, UnboundedMaxChildren, UnboundedMaxInFlight,
};

/// Spawn configuration selected by one actor type.
///
/// [`#[actor]`](macro@crate::actor) generates this implementation.
/// Manual configurations supply their own options carrier.
/// [`MessageConfig`](crate::MessageConfig) supplies messaging state.
/// [`SupervisionConfig`] completes the shape.
/// `Options` carries every per-spawn runtime choice.
/// Both configuration traits read the same value.
pub trait ActorConfig {
    /// Values resolved synchronously before the actor task starts.
    type Options: Default;
}

/// Configures one actor's direct-child supervision.
///
/// Custom configurations select one built-in [`supervision`] profile.
/// The profile owns child registrations and terminal events.
///
/// [`supervision`]: crate::supervision
pub trait SupervisionConfig: ActorConfig {
    /// The direct-child supervision profile.
    type Children: crate::supervision::ChildSupervisor;

    /// Opens this actor's direct-child supervision profile.
    fn open_children(options: &Self::Options) -> Self::Children;
}
