# loac

`loac` is a typed local-actor runtime built around `Cx`.
Handlers use `Cx::with` for temporary actor access.
Those borrows cannot cross suspension points.
This permits safe handler interleaving on one actor task.
Finite policies bound mailboxes, handlers, and children.
Structured supervision owns child lifecycles.

The name joins `Loong` and `Actor` (`lo` + `ac`).

## Quick Start

```rust
use loac::{ExitReason, Shutdown, SubtreeStatus, prelude::*};

struct Counter(u64);

#[actor(mailbox)]
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
| [`Cx`](https://docs.rs/loac/latest/loac/struct.Cx.html) | Lends actor access during handler polls. |

## Choose Capabilities

`#[actor(...)]` generates the runtime configuration.
Messaging and child ownership are opt-in.
Omitting a capability removes its methods.
Capability flags accept no values.

| Quantity policy | Selected profile |
| --- | --- |
| `N` | Fixed limit of N |
| `dynamic` | Per-spawn limit using the library default |
| `dynamic(N)` | Per-spawn limit defaulting to N |
| `unbounded` | No finite limit |

Every finite limit is a nonzero `usize` constant.

### Mailbox

`mailbox` enables typed messaging.
`mailbox_capacity` selects its admission limit.
The default capacity is `32`.

Mailbox capacity bounds messages awaiting dispatch.
It does not bound active handler futures. [`call`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.call) and [`send`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.send) wait when full.
[`try_call`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_call) and [`try_send`](https://docs.rs/loac/latest/loac/struct.ActorRef.html#method.try_send) return immediately.
Dynamic options expose [`with_mailbox_capacity`](https://docs.rs/loac/latest/loac/trait.DynamicMailboxCapacityOptions.html#tymethod.with_mailbox_capacity).

### Handler Concurrency

`max_in_flight` requires `mailbox`.

The limit counts active handler futures.
A full limit pauses dispatch before another handler starts.
The default limit is `32`.
Use `max_in_flight = 1` for strict serialization.
Dynamic options expose [`with_max_in_flight`](https://docs.rs/loac/latest/loac/trait.DynamicMaxInFlightOptions.html#tymethod.with_max_in_flight).

### Child-Spawning

`children` enables direct child ownership.
`max_children` selects its retained-child limit.
The default limit is `32`.

The limit counts retained child registrations.
Finite profiles return the original spawn inputs on [`Full`](https://docs.rs/loac/latest/loac/supervision/struct.Full.html).
Unbounded profiles use `Infallible` as their error.
Dynamic options expose [`with_max_children`](https://docs.rs/loac/latest/loac/trait.DynamicMaxChildrenOptions.html#tymethod.with_max_children).

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

## `Cx` Scheduling

Every message handler returns one future.
The actor task owns and polls every handler future.
Scheduling rotates across dispatch, handlers, and child exits.
`max_in_flight` bounds active handler futures.

`Handler` and `StreamHandler` receive a `Cx` handle.
`Cx::with` lends actor and scope through a synchronous closure.
Actor access ends when that closure returns.
`Cx::waker` returns a `Send + Sync` waker for the handler future.
Waking it polls that handler alone; late wakes are no-ops.
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
