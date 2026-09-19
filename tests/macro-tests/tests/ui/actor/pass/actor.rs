// The runtime dependency uses a Cargo alias.
// Both attribute forms must resolve that alias.
use std::num::NonZeroUsize;

use actor_api::{ActorConfig, MessageConfig, prelude::*};

trait Same<T> {}

impl<T> Same<T> for T {}

fn assert_same<T, U>()
where
    T: Same<U>,
{
}

struct Bare;

#[actor]
impl Actor for Bare {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct Messaging;

#[actor_api::actor(mailbox, mailbox_capacity = 8, mailbox_dispatch_budget = 3)]
impl Actor for Messaging {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct Supervisor;

#[actor_api::actor(children, max_children = unbounded)]
impl Actor for Supervisor {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct FixedSupervisor;

#[actor_api::actor(children, max_children = 8)]
impl Actor for FixedSupervisor {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct DynamicSupervisor;

#[actor_api::actor(children, max_children = dynamic)]
impl Actor for DynamicSupervisor {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct DynamicMailboxCapacity;

#[actor_api::actor(mailbox, mailbox_capacity = dynamic)]
impl Actor for DynamicMailboxCapacity {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct UnboundedMailboxCapacity;

#[actor_api::actor(mailbox, mailbox_capacity = unbounded)]
impl Actor for UnboundedMailboxCapacity {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct UnboundedMaxInFlight;

#[actor_api::actor(mailbox, max_in_flight = unbounded)]
impl Actor for UnboundedMaxInFlight {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct DefaultCapabilities;

#[actor_api::actor(mailbox, children)]
impl Actor for DefaultCapabilities {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

const MAILBOX_CAPACITY: usize = 16;
const MAILBOX_DISPATCH_BUDGET: usize = 5;

mod limits {
    pub const CHILD_CAPACITY: usize = 8;
}

struct Combined;

#[actor_api::actor(
    mailbox, mailbox_capacity = MAILBOX_CAPACITY,
    mailbox_dispatch_budget = MAILBOX_DISPATCH_BUDGET,
    max_in_flight = 1 << 2,
    children, max_children = dynamic(limits::CHILD_CAPACITY),
)]
impl Actor for Combined {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct Generic<T, const N: usize>(T);

#[actor_api::actor(
    mailbox, mailbox_capacity = dynamic(N),
    mailbox_dispatch_budget = N,
    max_in_flight = dynamic(N),
    children, max_children = N,
)]
impl<T, const N: usize> Actor for Generic<T, N>
where
    T: Send + 'static,
{
    type SpawnArgs = T;

    async fn init(value: T, _: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

// Generated companion items must inherit conditional compilation.
#[cfg(any())]
struct Conditional;

struct ConditionalAttribute;

#[actor_api::actor(mailbox, mailbox_capacity = dynamic)]
#[cfg(any())]
impl Actor for Conditional {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[cfg_attr(any(), cfg(any()))]
#[actor_api::actor(mailbox, mailbox_capacity = unbounded)]
impl Actor for ConditionalAttribute {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn override_mailbox_capacity<A>(options: A::Options) -> A::Options
where
    A: ActorConfig,
    A::Options: DynamicMailboxCapacityOptions,
{
    options.with_mailbox_capacity(NonZeroUsize::MIN)
}

fn override_max_in_flight<A>(options: A::Options) -> A::Options
where
    A: ActorConfig,
    A::Options: DynamicMaxInFlightOptions,
{
    options.with_max_in_flight(NonZeroUsize::MIN)
}

fn main() {
    assert_same::<<Bare as MessageConfig>::Scheduler, actor_api::scheduling::Disabled>();
    assert_same::<
        <Messaging as MessageConfig>::Scheduler,
        actor_api::scheduling::Fixed<Messaging, 32>,
    >();
    assert_same::<
        <DefaultCapabilities as MessageConfig>::Scheduler,
        actor_api::scheduling::Fixed<DefaultCapabilities, 32>,
    >();
    assert_same::<
        <Generic<u8, 6> as MessageConfig>::Scheduler,
        actor_api::scheduling::Dynamic<Generic<u8, 6>>,
    >();
    assert_same::<
        <UnboundedMailboxCapacity as MessageConfig>::Sender,
        actor_api::transport::UnboundedSender<UnboundedMailboxCapacity>,
    >();
    assert_same::<
        <UnboundedMailboxCapacity as MessageConfig>::Inbox,
        actor_api::transport::UnboundedInbox<UnboundedMailboxCapacity>,
    >();
    assert_same::<
        <UnboundedMaxInFlight as MessageConfig>::Scheduler,
        actor_api::scheduling::Unbounded<UnboundedMaxInFlight>,
    >();

    assert_eq!(Messaging::MAILBOX_DISPATCH_BUDGET.get(), 3);
    assert_eq!(
        Combined::MAILBOX_DISPATCH_BUDGET.get(),
        MAILBOX_DISPATCH_BUDGET
    );
    assert_eq!(Generic::<u8, 6>::MAILBOX_DISPATCH_BUDGET.get(), 6);

    let _ = actor_api::SpawnOptions::<DynamicMailboxCapacity>::default()
        .with_mailbox_capacity(NonZeroUsize::MIN);
    let _ = override_mailbox_capacity::<Generic<u8, 6>>(
        actor_api::SpawnOptions::<Generic<u8, 6>>::default(),
    );
    let _ = override_max_in_flight::<Generic<u8, 6>>(
        actor_api::SpawnOptions::<Generic<u8, 6>>::default(),
    );
    let _ = actor_api::SpawnOptions::<DynamicSupervisor>::default()
        .with_max_children(NonZeroUsize::MIN);
}
