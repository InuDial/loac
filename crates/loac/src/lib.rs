#![deny(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Typed local actors with bounded messaging and structured supervision.
//!
//! An [`Actor`] owns mutable state.
//! Its lifecycle hooks run serially.
//! Communication stays inside one process.
//! Actor tasks are `Send` and run on Tokio.
//! Messaging and child actor ownership are opt-in.
//!
//! # Quick start
//!
//! This actor accepts typed `Add` requests.
//! [`Handler`] produces each reply through an actor-access `cx` future.
//!
//! ```
//! use loac::{ExitReason, Shutdown, prelude::*};
//!
//! struct Counter(u64);
//!
//! #[actor(mailbox)]
//! impl Actor for Counter {
//!     type SpawnArgs = u64;
//!
//!     async fn init(initial: u64, _scope: &mut ActorScope<'_, Self>) -> Self {
//!         Self(initial)
//!     }
//! }
//!
//! #[derive(Message)]
//! #[message(reply = u64)]
//! struct Add(u64);
//!
//! impl Handler<Add> for Counter {
//!     async fn handle(message: Add, mut cx: Cx<'_, Self>) -> u64 {
//!         cx.with(|actor, _| {
//!             actor.0 += message.0;
//!             actor.0
//!         })
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let owner = loac::spawn::<Counter>(0);
//!
//!     assert_eq!(owner.call(Add(2)).await?, 2);
//!     let status = owner.shutdown(Shutdown::Drain).await;
//!     assert_eq!(status.reason(), ExitReason::Drained);
//!     Ok(())
//! }
//! ```
//!
//! [`spawn`] returns before [`Actor::init`] completes.
//! Messages may enter the mailbox during initialization.
//! Handler dispatch starts after initialization.
//! The [`ActorOwner`] owns the root actor's lifecycle.
//!
//! # Choose actor capabilities
//!
//! Most actors use [`#[actor(...)]`](actor) on their [`Actor`] implementation.
//! The attribute generates the built-in runtime configuration.
//! Its reference documents every syntax, default, and constraint.
//! The generated [`MessageConfig`] opens three matched states.
//! They are [`Sender`](MessageConfig::Sender), [`Inbox`](MessageConfig::Inbox),
//! and [`Scheduler`](MessageConfig::Scheduler).
//! Without `mailbox`, the scheduler is [`Disabled`](scheduling::Disabled).
//! Without `max_in_flight`, a mailbox uses [`Fixed`](scheduling::Fixed).
//!
//! | Option | Purpose | When omitted |
//! | --- | --- | --- |
//! | `mailbox` | Enables typed [`send`](ActorRef::send) and [`call`](ActorRef::call) | Messaging methods are unavailable |
//! | `mailbox_capacity = P` | Selects mailbox capacity | Uses `32` |
//! | `mailbox_dispatch_budget = E` | Limits consecutive message dispatch | Uses `16` with a mailbox |
//! | `max_in_flight = P` | Limits active handlers | Uses `32` |
//! | `children` | Enables [`spawn_child`](ActorScope::spawn_child) | The method is unavailable |
//! | `max_children = P` | Limits retained children | Uses `32` |
//!
//! Capability flags accept no values.
//! Quantity policies support fixed, dynamic, and unbounded limits.
//! Dynamic profiles expose spawn-specific overrides:
//!
//! - [`with_mailbox_capacity`](DynamicMailboxCapacityOptions::with_mailbox_capacity);
//! - [`with_max_in_flight`](DynamicMaxInFlightOptions::with_max_in_flight);
//! - [`with_max_children`](DynamicMaxChildrenOptions::with_max_children).
//!
//! Pass changed [`SpawnOptions`] to [`spawn_with`].
//!
//! # Manual no-mailbox configuration
//!
//! The [`#[actor(...)]`](actor) attribute covers ordinary actors.
//! Manual configurations exist for custom transports.
//! A mailbox manual config supplies its own transport.
//! A no-mailbox manual config may reuse the built-in states.
//! Implement [`ActorConfig`], [`MessageConfig`], and [`SupervisionConfig`].
//! Use [`transport::NoSender`], [`transport::NoInbox`], and
//! [`scheduling::Disabled`].
//!
//! A manual config may also supply its own options carrier.
//! Normally the empty carrier (`()`) suffices.
//!
//! # Core model
//!
//! | Type | Role |
//! | --- | --- |
//! | [`Actor`] | Owns state and serial lifecycle hooks |
//! | [`ActorRef`] | Provides a cloneable, typed non-owning handle |
//! | [`Recipient`] | Erases the actor type for one message capability |
//! | [`ActorOwner`] | Uniquely owns one root actor |
//! | [`ActorScope`] | Exposes temporary capabilities during actor work |
//!
//! An [`ActorRef`] may request shutdown.
//! It sends messages only when the actor has [`HasMailbox`].
//! Keeping an [`ActorRef`] does not keep its actor alive.
//! A [`Recipient`] handle has only one message capability and no lifecycle methods.
//!
//! # Messages
//!
//! Derive [`Message`] for each accepted request type. The derive supports two
//! message shapes:
//!
//! | Attribute | Handler trait | Caller receives |
//! | --- | --- | --- |
//! | `#[message(reply = Type)]` | [`Handler`] | `Type` |
//! | `#[message(stream = Item, reply = Final)]` | [`StreamHandler`] | [`StreamReply`]`<Item, Final>` |
//!
//! Omitting the `#[message(...)]` attribute entirely produces a send-only
//! message with unit output. Selecting either `reply` or `stream` implements
//! [`HasReply`] and makes the message callable with [`ActorRef::call`]; the
//! caller still receives `Result<M::Reply, CallError>`. Reply and final types
//! default to `()` when omitted. Stream messages are handled by
//! [`StreamHandler`].
//!
//! One actor may handle many message types.
//!
//! [`ActorRef::call`] waits for acceptance and a typed reply.
//! [`ActorRef::send`] waits only for one-way message acceptance.
//! [`ActorRef::recipient`] creates a type-erased handle for one message type.
//! The `try_` variants never wait for mailbox capacity.
//! Their errors retain messages that were not accepted.
//! Method docs describe cancellation and shutdown races.
//!
//! # Reply progress
//!
//! Every handler returns one future.
//! The actor task owns and polls every handler future.
//! Mailbox configuration limits active handler futures.
//! Omitting `max_in_flight` permits `32` active handlers.
//! A cx future accesses actor state through [`Cx::with`].
//! [`Cx::waker`] returns a `Send + Sync` waker for that future.
//! Waking it polls that handler alone; late wakes are no-ops.
//! [`Cx::exclusive`] returns a scoped scheduler lease.
//! All scheduled actor work pauses until that guard drops.
//! Graceful `on_shutdown` hooks may still preempt the lease.
//! See [`reply`] for cancellation, panic, and scheduling details.
//! See [`scheduling`] for built-in scheduling profiles.
//!
//! # Ownership and child actors
//!
//! Each root actor has one [`ActorOwner`].
//! [`ActorScope::spawn_child`] starts a direct child actor.
//! The parent runtime owns that child actor.
//! Child spawning requires [`HasChildren`].
//! A [`Child`] is a typed, non-owning handle.
//! Parent shutdown reaches every owned child actor.
//!
//! Ownership forms a tree.
//! Actor references may cross tree boundaries.
//! They may also form communication cycles.
//! See [`supervision`] for built-in child actor profiles.
//!
//! # Shutdown and completion
//!
//! Every shutdown mode closes new message acceptance.
//!
//! | Mode | Behavior |
//! | --- | --- |
//! | [`Stop`](Shutdown::Stop) | Finishes dispatched replies and discards queued messages |
//! | [`Drain`](Shutdown::Drain) | Dispatches eligible queued messages and finishes resulting replies |
//! | [`Kill`](Shutdown::Kill) | Cancels cooperative work and skips graceful cleanup |
//!
//! Kill takes effect between polls.
//! It cannot interrupt synchronous code or user destructors.
//! [`Shutdown`] documents the complete retained-work contract.
//!
//! [`ActorOwner::shutdown`] requests a mode and waits.
//! [`ActorOwner::wait`] waits without requesting shutdown.
//! [`ActorRef::closed`] only observes actor termination.
//! [`ExitStatus`] separates local reason from subtree confirmation.
//!
//! # Progress boundaries
//!
//! Initialization and lifecycle hooks are serial.
//! They pause handler dispatch while pending.
//! Awaiting a self-call requires a later mailbox dispatch.
//! It cannot complete during initialization or a scheduler lease.
//! Communication cycles can therefore wait indefinitely.
//! Use [`Cx::with`] between awaits for local sequencing.
//!
//! # Advanced configuration
//!
//! The `actor` attribute covers built-in runtime shapes.
//! Custom configurations implement [`ActorConfig`] and [`MessageConfig`].
//! They also implement [`SupervisionConfig`].
//! [`MessageConfig`] opens transport and reply scheduling together.
//! Choose public profiles from [`scheduling`] and [`supervision`].
//! The [`transport`] module documents custom message transports.
//!
//! # Imports
//!
//! [`prelude`] contains actor-definition traits and extension methods.
//! Runtime operations remain explicit imports.
//! This keeps lifecycle choices visible at call sites.
//! The [examples index] lists runnable guides.
//!
//! [examples index]: https://github.com/InuDial/loac/blob/loac-v0.4.0/crates/loac/examples/README.md

// Derives use this name inside the runtime package.
// External callers may still rename their dependency.
extern crate self as loac;

use std::{future::Future, pin::Pin};

mod access;
mod actor;
mod address;
mod config;
mod error;
mod lifecycle;
mod mailbox;
pub mod reply;
mod runtime;
pub mod scheduling;
pub mod supervision;
pub mod transport;
mod writer;

pub use access::{Cx, ExclusiveGuard};
pub use actor::{Actor, Handler, HasChildren, HasMailbox, HasReply, Message, StreamHandler};
pub use address::{ActorRef, Recipient, Response};
pub use config::{
    ActorConfig, DynamicMailboxCapacityOptions, DynamicMaxChildrenOptions,
    DynamicMaxInFlightOptions, SupervisionConfig,
};
pub use error::{
    CallError, SendError, SendToError, TryCallError, TryCallErrorKind, TrySendError,
    TrySendErrorKind,
};
pub use lifecycle::{
    Child, ChildExit, ChildId, ExitReason, ExitStatus, Shutdown, ShutdownStatus, SubtreeStatus,
};
pub use loac_macros::{Message, actor};
pub use reply::{Items, StreamMessage, StreamReply};
pub use runtime::{
    ActorOwner, ActorScope, ActorSpawner, SpawnOptions, StopScope, spawn, spawn_with,
};
pub use transport::MessageConfig;
pub use writer::{StreamOut, Writer};

// The macro references these through `__private`.
// The root re-export keeps rustc diagnostics free of `__private` paths.
#[doc(hidden)]
pub use config::{
    ActorOptions, DynamicMailboxCapacity, DynamicMaxChildren, DynamicMaxInFlight,
    FixedMailboxCapacity, FixedMaxChildren, FixedMaxInFlight, NoChildren, NoMailbox, NoMaxInFlight,
    UnboundedMailboxCapacity, UnboundedMaxChildren, UnboundedMaxInFlight,
};

/// Implementation details used by generated actor configuration.
#[doc(hidden)]
pub mod __private {
    pub use crate::config::{
        ActorOptions, DEFAULT_MAILBOX_CAPACITY, DEFAULT_MAX_CHILDREN, DEFAULT_MAX_IN_FLIGHT,
        DynamicMailboxCapacity, DynamicMaxChildren, DynamicMaxInFlight, FixedMailboxCapacity,
        FixedMaxChildren, FixedMaxInFlight, NoChildren, NoMailbox, NoMaxInFlight,
        UnboundedMailboxCapacity, UnboundedMaxChildren, UnboundedMaxInFlight,
    };
    pub use crate::transport::{
        BoundedInbox, BoundedSender, NoInbox, NoSender, UnboundedInbox, UnboundedSender,
    };
}

/// Common traits and types for defining actors and handlers.
///
/// This prelude deliberately stops at the actor definition boundary. Runtime
/// entry points, ownership handles, lifecycle controls, addresses, and errors
/// remain explicit imports so operational behavior stays visible at call sites.
pub mod prelude {
    pub use crate::{
        Actor, ActorScope, ActorSpawner, Cx, DynamicMailboxCapacityOptions,
        DynamicMaxChildrenOptions, DynamicMaxInFlightOptions, Handler, HasChildren, HasMailbox,
        HasReply, Items, Message, StopScope, StreamHandler, StreamMessage, StreamOut, StreamReply,
        Writer, actor, reply,
    };
}

// Heap type erasure is confined to heterogeneous scheduler/mailbox ownership
// and the once-per-actor task wrapper. Public reply construction stays generic.
pub(crate) type ErasedFuture<'a, T = ()> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(test)]
mod message_derive_tests {
    #[derive(crate::Message)]
    struct InternalMessage;

    // Unit targets make proc-macro-crate return `Itself`.
    // This proves expansions use the stable runtime alias.
    #[test]
    fn derive_resolves_runtime_package() {
        // This helper makes reply mismatches fail compilation.
        fn assert_message<M: crate::Message<Reply = ()>>() {}

        assert_message::<InternalMessage>();
    }
}
