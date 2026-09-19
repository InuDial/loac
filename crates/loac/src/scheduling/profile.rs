#![allow(
    private_interfaces,
    reason = "private scheduler work seals the public capability trait"
)]

use std::num::NonZeroUsize;

use crate::{
    Actor,
    transport::{MessageConfig, MessageInbox, MessageSender, NoInbox, NoSender},
};

use super::runtime;
use super::{DynamicLimit, FixedLimit, ReplyState, UnboundedLimit};

/// A sealed runtime scheduling profile for one actor.
///
/// Custom configurations select a built-in profile.
/// They do not implement this trait directly.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot schedule replies for `{A}`",
    label = "select a scheduler compatible with this actor's capabilities"
)]
#[allow(
    private_bounds,
    private_interfaces,
    reason = "a private runtime bridge seals reply profiles"
)]
pub trait SchedulerProfile<A: Actor>: Send + 'static + Sized {
    /// Private runtime selected by this profile.
    #[doc(hidden)]
    type Runtime: runtime::ProfileRuntime<A, Self>;
}

/// The scheduler for actors without a mailbox.
///
/// This profile is zero-sized and schedules nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Disabled;

impl Disabled {
    /// Creates a disabled profile.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Schedules one active reply at a time.
pub struct Serial<A: Actor> {
    pub(super) state: ReplyState<A, FixedLimit<1>>,
}

impl<A: Actor> Serial<A> {
    /// Creates an empty serial profile.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ReplyState::with_limit(FixedLimit),
        }
    }
}

impl<A: Actor> Default for Serial<A> {
    fn default() -> Self {
        Self::new()
    }
}

/// Schedules at most `N` active handler futures.
pub struct Fixed<A: Actor, const N: usize> {
    pub(super) state: ReplyState<A, FixedLimit<N>>,
}

impl<A: Actor, const N: usize> Fixed<A, N> {
    /// Creates an empty fixed profile.
    ///
    /// Compilation fails when `N` is zero.
    #[must_use]
    pub fn new() -> Self {
        const { assert!(N > 0, "handler concurrency must be greater than zero") };
        Self {
            state: ReplyState::with_limit(FixedLimit),
        }
    }
}

impl<A: Actor, const N: usize> Default for Fixed<A, N> {
    fn default() -> Self {
        Self::new()
    }
}

/// Schedules handler futures with a per-spawn limit.
pub struct Dynamic<A: Actor> {
    pub(super) state: ReplyState<A, DynamicLimit>,
}

impl<A: Actor> Dynamic<A> {
    /// Creates an empty profile with one resolved limit.
    #[must_use]
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            state: ReplyState::with_limit(DynamicLimit(limit)),
        }
    }
}

/// Schedules handler futures without a finite limit.
pub struct Unbounded<A: Actor> {
    pub(super) state: ReplyState<A, UnboundedLimit>,
}

impl<A: Actor> Unbounded<A> {
    /// Creates an empty unbounded profile.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ReplyState::with_limit(UnboundedLimit),
        }
    }
}

impl<A: Actor> Default for Unbounded<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> SchedulerProfile<A> for Disabled
where
    A: Actor + MessageConfig<Sender = NoSender, Inbox = NoInbox, Scheduler = Self>,
{
    type Runtime = runtime::DisabledRuntime;
}

impl<A> SchedulerProfile<A> for Serial<A>
where
    A: Actor + MessageConfig<Scheduler = Self>,
    A::Sender: MessageSender<A>,
    A::Inbox: MessageInbox<A>,
{
    type Runtime = runtime::MailboxRuntime;
}

impl<A, const N: usize> SchedulerProfile<A> for Fixed<A, N>
where
    A: Actor + MessageConfig<Scheduler = Self>,
    A::Sender: MessageSender<A>,
    A::Inbox: MessageInbox<A>,
{
    type Runtime = runtime::MailboxRuntime;
}

impl<A> SchedulerProfile<A> for Dynamic<A>
where
    A: Actor + MessageConfig<Scheduler = Self>,
    A::Sender: MessageSender<A>,
    A::Inbox: MessageInbox<A>,
{
    type Runtime = runtime::MailboxRuntime;
}

impl<A> SchedulerProfile<A> for Unbounded<A>
where
    A: Actor + MessageConfig<Scheduler = Self>,
    A::Sender: MessageSender<A>,
    A::Inbox: MessageInbox<A>,
{
    type Runtime = runtime::MailboxRuntime;
}
