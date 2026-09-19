# loac

`loac` is a small runtime for typed, local actors. It combines Actix-style message/reply typing with bounded admission, explicit ownership, and a Ractor-style supervision tree.

The name joins `Loong` and `Actor` (`lo` + `ac`).

The crate is an early MVP with a deliberately narrow contract.

## Quick Start

```rust
use loac::{ExitReason, Shutdown, SubtreeStatus, prelude::*};

struct Counter(u64);

#[actor(mailbox, interleaved = unbounded)]
impl Actor for Counter {
    type SpawnArgs = u64;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Add(u64);

impl Handler<Add> for Counter {
    async fn handle(message: Add, mut cx: Cx<'_, Self>) -> u64 {
        cx.with(|actor, _| {
            actor.0 += message.0;
            actor.0
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = loac::spawn::<Counter>(0);

    assert_eq!(owner.call(Add(2)).await?, 2);
    assert_eq!(owner.call(Add(3)).await?, 5);
    let status = owner.shutdown(Shutdown::Drain).await;
    assert_eq!(status.reason(), ExitReason::Drained);
    assert_eq!(status.subtree(), SubtreeStatus::Terminated);
    Ok(())
}
```

See the [examples index](examples/README.md) for runnable guides.

## Core Model

| Type | Role |
| --- | --- |
| [`Actor`](https://docs.rs/loac/latest/loac/trait.Actor.html) | Owns state. Lifecycle hooks run serially. |
| [`ActorRef`](https://docs.rs/loac/latest/loac/struct.ActorRef.html) | Cloneable, typed, non-owning handle. |
| [`Recipient`](https://docs.rs/loac/latest/loac/trait.Recipient.html) | Erases the actor type for one message type. |
| [`ActorOwner`](https://docs.rs/loac/latest/loac/struct.ActorOwner.html) | Uniquely owns one root actor. |
| [`ActorScope`](https://docs.rs/loac/latest/loac/struct.ActorScope.html) | Exposes temporary capabilities during actor work. |

## Choose Capabilities

`#[actor(...)]` generates the runtime configuration.
Messaging and child ownership are opt-in.
Omitting an option removes that capability.
`mailbox`, `interleaved`, and `children` share the five forms below.
An explicit finite limit must be a nonzero `usize` constant.

| Form | Selected profile |
| --- | --- |
| bare `option` | Fixed limit of 32 |
| `option = N` | Fixed limit of N |
| `option = dynamic` | Per-spawn limit defaulting to 32 |
| `option = dynamic(N)` | Per-spawn limit defaulting to N |
| `option = unbounded` | No finite limit |

### Mailbox

Here `option` is `mailbox`.

Mailbox capacity bounds messages awaiting dispatch.
It does not bound active replies. [`call`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.call) and [`send`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.send) wait when full.
[`try_call`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_call) and [`try_send`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_send) return immediately.
Dynamic options expose [`with_mailbox_capacity`](https://docs.rs/loac/latest/loac/trait.DynamicMailboxOptions.html#tymethod.with_mailbox_capacity).

### Handler Concurrency

Here `option` is `interleaved`. It requires `mailbox`.

The limit counts active handler futures.
A full limit pauses dispatch before another handler starts.
Omitting `interleaved` permits one active handler.
Dynamic options expose [`with_max_in_flight`](https://docs.rs/loac/latest/loac/trait.DynamicInterleavingOptions.html#tymethod.with_max_in_flight).

### Child-Spawning

Here `option` is `children`.

The limit counts retained child registrations.
Finite profiles return the original spawn inputs on [`Full`](https://docs.rs/loac/latest/loac/supervision/struct.Full.html).
Unbounded profiles use `Infallible` as their error.
Dynamic options expose [`with_max_children`](https://docs.rs/loac/latest/loac/trait.DynamicChildrenOptions.html#tymethod.with_max_children).

See the [attribute reference](https://docs.rs/loac/latest/loac/attr.actor.html) for syntax and constraints.

## Message Shapes

`#[derive(Message)]` supports two message shapes:

| Attribute | Handler trait | Caller receives |
| --- | --- | --- |
| `#[message(reply = Type)]` | [`Handler`](https://docs.rs/loac/latest/loac/trait.Handler.html) | `Type` |
| `#[message(stream = Item, reply = Final)]` | [`StreamHandler`](https://docs.rs/loac/latest/loac/trait.StreamHandler.html) | `StreamReply<Item, Final>` |

Use `#[message(reply = Type)]` and `Handler<M>`.
Declare the eventual result as `Type`.
This includes `Result<Value, Error>` when appropriate.
The actor task owns and polls the returned future.

Omitting the `#[message(...)]` attribute entirely produces a send-only
message with unit output. Selecting either `reply` or `stream` implements
`HasReply` and makes the message callable with `ActorRef::call`; the caller
still receives `Result<M::Reply, CallError>`. Reply and final types default
to `()` when omitted.

## Reply Scheduling

Every message handler returns one future.
The actor task schedules all handler futures fairly.
Mailbox profiles limit their active count.

`Handler` and `StreamHandler` receive a `Cx` handle.
Call `Cx::with` for temporary actor and scope access.
Call `Cx::exclusive` for a scoped scheduler lease.
All scheduled actor work pauses until that guard drops.
Graceful `on_shutdown` hooks may still preempt the lease.

See the [`Cx` safety argument](SAFETY.md) for unsafe invariants.

Stream messages use `#[message(stream = Item, reply = Final)]`. The runtime
creates a bounded item channel and returns the receiver to the caller as a
`StreamReply`. `StreamHandler` produces items through its `StreamOut` writer.
The item stream ends when the handler drops its writer.

## Lifecycle and Shutdown

Graceful shutdown runs post-order through the owned tree.

| Mode | Behavior |
| --- | --- |
| `Stop` | Finishes dispatched replies and discards queued messages. |
| `Drain` | Dispatches eligible queued messages and finishes their replies. |
| `Kill` | Cancels cooperative work and skips `on_stop`. |

Shutdown closes admission when it commits.
A later [`call`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.call) returns [`CallError::Closed`](https://docs.rs/loac/latest/loac/enum.CallError.html#variant.Closed).
[`ExitStatus::reason`](https://docs.rs/loac/latest/loac/struct.ExitStatus.html) describes only that actor.
`subtree` reports whether the runtime confirmed every owned descendant terminated.
`Unconfirmed` means proof is unavailable. It stays sticky through ancestors and does not stop a running parent.
Kill takes effect between polls. It cannot interrupt a synchronous handler, a poll that never returns, or user `Drop`.

## Progress Boundaries

- [`spawn`](https://docs.rs/loac/latest/loac/fn.spawn.html) schedules `init` and returns immediately. Admission opens before `init` finishes.
- `init` and lifecycle hooks run serially and block dispatch.
- Mailbox FIFO decides dispatch order. Async replies may complete in a different order.
- A self-call needs fresh dispatch capacity. Leases pause that dispatch.
- Use `Cx::with` between awaits for consecutive actor work.
- Address cycles can deadlock when every participant waits.

Licensed under the MIT License.
