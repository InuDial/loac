#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Procedural macros for `loac`.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;

mod actor;
mod message;

/// Configures one `impl Actor for Type` block.
///
/// The attribute generates these runtime configuration implementations:
///
/// - [`ActorConfig`][actor-config];
/// - [`MessageConfig`][message-config];
/// - [`SupervisionConfig`][supervision-config].
///
/// The generated `MessageConfig` opens the actor's `Sender`, `Inbox`, and
/// `Scheduler` state.
/// Do not implement those traits again for the same actor.
/// A bare attribute enables no optional capability.
///
/// ```
/// # use actor_api as loac;
/// use loac::{Actor, ActorScope, actor};
///
/// struct Worker;
///
/// #[actor(
///     mailbox,
///     mailbox_capacity = dynamic,
///     max_in_flight = dynamic,
///     children,
///     max_children = 4,
/// )]
/// impl Actor for Worker {
///     type SpawnArgs = ();
///
///     async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
///         Self
///     }
/// }
/// ```
///
/// # Options
///
/// | Option | Purpose | Requires |
/// | --- | --- | --- |
/// | `mailbox` | Enables typed public messaging | Nothing |
/// | `mailbox_capacity = P` | Selects mailbox capacity | `mailbox` |
/// | `mailbox_dispatch_budget = E` | Limits consecutive dispatch | `mailbox` |
/// | `max_in_flight = P` | Limits active handlers | `mailbox` |
/// | `children` | Enables direct child ownership | Nothing |
/// | `max_children = P` | Limits retained children | `children` |
///
/// Capability flags accept no values.
/// Quantity policies use these forms:
///
/// | Form | Selected profile |
/// | --- | --- |
/// | `N` | Fixed limit of `N` |
/// | `dynamic` | Per-spawn limit using the library default |
/// | `dynamic(N)` | Per-spawn limit defaulting to `N` |
/// | `unbounded` | No finite limit |
///
/// Every finite expression must produce a nonzero `usize` constant.
/// Dynamic forms expose one method on [`SpawnOptions`][spawn-options]:
///
/// - [`with_mailbox_capacity`][mailbox-builder] for `mailbox_capacity`;
/// - [`with_max_in_flight`][max-in-flight-builder] for `max_in_flight`;
/// - [`with_max_children`][children-builder] for `max_children`.
///
/// `mailbox_capacity = dynamic` defaults to `32`.
/// `max_in_flight = dynamic` defaults to `32`.
/// `max_children = dynamic` defaults to `32`.
///
/// # Mailbox
///
/// Mailbox capacity bounds accepted messages awaiting dispatch.
/// It does not bound active replies.
/// [`ActorRef::call`][call] and [`ActorRef::send`][send] wait when full.
/// [`ActorRef::try_call`][try-call] and [`ActorRef::try_send`][try-send] return immediately.
/// Omitting `mailbox_capacity` uses `32`.
/// A mailbox without `max_in_flight` uses [`Fixed`][fixed] with `32`.
/// Omitting `mailbox` removes public messaging methods.
/// It selects zero-sized [`Disabled`][disabled].
///
/// # Mailbox dispatch budget
///
/// `mailbox_dispatch_budget = E` accepts a nonzero `usize` const expression.
/// It defaults to `16` when omitted.
/// At most `E` messages dispatch before checking other actor work.
/// This check does not force a Tokio task yield.
/// The option requires `mailbox`.
///
/// # Handler concurrency
///
/// The limit counts active handler futures.
/// At the limit, queued messages pause before handler dispatch.
/// Omitting `max_in_flight` permits `32` active handlers.
/// The option requires `mailbox`.
///
/// # Child actors
///
/// The limit counts direct child actors retained by the parent.
/// An exited child actor remains counted until its slot is released.
/// That release occurs before [`Actor::on_child_exit`][on-child-exit] starts.
/// A finite rejection returns the original spawn input.
/// Unbounded spawning uses [`Infallible`](std::convert::Infallible) as its error.
/// Omitting `children` removes child actor spawning methods.
/// Omitting `max_children` uses `32`.
/// This option does not require `mailbox`.
///
/// [actor-config]: https://docs.rs/loac/latest/loac/trait.ActorConfig.html
/// [disabled]: https://docs.rs/loac/latest/loac/scheduling/struct.Disabled.html
/// [message-config]: https://docs.rs/loac/latest/loac/trait.MessageConfig.html
/// [fixed]: https://docs.rs/loac/latest/loac/scheduling/struct.Fixed.html
/// [supervision-config]: https://docs.rs/loac/latest/loac/trait.SupervisionConfig.html
/// [spawn-options]: https://docs.rs/loac/latest/loac/type.SpawnOptions.html
/// [mailbox-builder]: https://docs.rs/loac/latest/loac/trait.DynamicMailboxCapacityOptions.html#tymethod.with_mailbox_capacity
/// [max-in-flight-builder]: https://docs.rs/loac/latest/loac/trait.DynamicMaxInFlightOptions.html#tymethod.with_max_in_flight
/// [children-builder]: https://docs.rs/loac/latest/loac/trait.DynamicMaxChildrenOptions.html#tymethod.with_max_children
/// [call]: https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.call
/// [send]: https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.send
/// [try-call]: https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_call
/// [try-send]: https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_send
/// [on-child-exit]: https://docs.rs/loac/latest/loac/trait.Actor.html#method.on_child_exit
#[proc_macro_attribute]
pub fn actor(args: TokenStream, input: TokenStream) -> TokenStream {
    actor::expand(args, input)
}

/// Derives `loac::Message` for a struct, enum, or union.
///
/// A message derived without a `#[message(...)]` attribute is send-only with
/// unit output. Selecting either `reply` or `stream` makes the message
/// callable through `loac::HasReply`. The reply type defaults to `()` when
/// `reply` is omitted.
///
/// Use `#[message(reply = Type)]` with `loac::Handler`.
/// The actor task owns and polls the returned future.
///
/// Use `#[message(stream = Item, reply = Final)]` for a streamed reply:
/// `loac::call` then returns `loac::StreamReply<Item, Final>`, and the message
/// is handled by implementing `loac::StreamHandler`. The final reply type
/// defaults to `()` when only `stream` is present.
///
/// Generic parameters and existing `where` predicates are preserved. The derive
/// adds `Send + 'static` bounds to the message type and every selected reply
/// or stream item type.
#[proc_macro_derive(Message, attributes(message))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    message::expand(input)
}

fn actor_crate_path() -> syn::Result<TokenStream2> {
    // Proc macros lack `$crate`.
    // Resolve the package after dependency renaming.
    match crate_name("loac").map_err(|error| {
        syn::Error::new(
            Span::call_site(),
            format!("could not resolve the `loac` crate: {error}"),
        )
    })? {
        // `crate` may name a package binary or example.
        // The runtime exports one stable self alias.
        FoundCrate::Itself => Ok(quote!(::loac)),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, Span::call_site());
            Ok(quote!(::#ident))
        }
    }
}
