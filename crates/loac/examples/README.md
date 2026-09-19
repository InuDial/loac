# Examples for loac

These examples are small, executable guides to `loac`.
Start with [`cx_reply`](cx_reply.rs).

Run an example from the workspace root:

```console
cargo run -p loac --example cx_reply
```

Replace `cx_reply` with any target listed below.
Most examples print nothing.
Their assertions check the demonstrated behavior.
`streaming` prints receive times to show items arriving during production.

## Message handling

| Example | Focus |
| --- | --- |
| [`cx_reply`](cx_reply.rs) | Primary `Handler` and `StreamHandler` with actor-access `cx` futures. |

Use `Handler<M>` with an async `cx` future.

`Handler` and `StreamHandler` receive a `Cx` handle.
Use `Cx::with` for temporary actor access.
Use `Cx::exclusive` for a scoped scheduler lease.

## Streaming

A stream message derives `#[message(stream = Item, reply = Final)]`. The runtime
creates a bounded item channel and returns the receiver side to the caller as a
`StreamReply`; the handler receives the sender side as a generic `Writer`.

| Example | Focus |
| --- | --- |
| [`stream_to`](stream_to.rs) | Primary `StreamHandler`: caller-provided writers through `call_to`/`send_to`. |
| [`streaming`](dispatch/streaming.rs) | Stream items through the runtime channel. |

`call` returns a `StreamReply`. Read items with `recv` or `items`, then `finish`
returns the final value. The item stream closes when the handler drops the
writer. `finish` is final-aware and discards remaining buffered items.

For a caller-provided writer such as an `ActorRef`, use `call_to` or `send_to`.
They pass the writer straight to the handler without creating an item channel,
so `call_to` returns the final value directly and `send_to` is one-way.

## Actor configuration

Each capability selects its limit profile independently.

| Example | Focus |
| --- | --- |
| [`actor_configuration`](configuration/actor_configuration.rs) | Mixed limit profiles, one spawn override, and dispatch budget. |
| [`const_generic_configuration`](configuration/const_generic_configuration.rs) | Reuse a const generic as a fixed limit and dynamic default. |

`SpawnOptions` changes one actor spawn.
Dynamic profiles expose per-spawn overrides.
Unbounded removes only the selected finite limit.
Applications remain responsible for resource growth.
`mailbox_budget` belongs to the actor type.

## Exclusive scheduling

| Example | Focus |
| --- | --- |
| [`cx_exclusive`](dispatch/cx_exclusive.rs) | Use scheduler leases in regular and streaming handlers. |

Use `Cx::exclusive` for scoped actor isolation.

The bare `interleaved` option uses a fixed limit of 32.
Dynamic options allow `with_max_in_flight` per spawn.
Omitting the option permits one active handler.
Unbounded concurrency can retain arbitrarily many handler futures.

## Actor topology

Lifecycle ownership and message addresses form different graphs.

| Example | Focus |
| --- | --- |
| [`top_level_actors`](topology/top_level_actors.rs) | Own independent roots and drop one owner. |
| [`child_actors`](topology/child_actors.rs) | Own child actors and gather their replies. |
| [`address_cycle`](topology/address_cycle.rs) | Build an address cycle during actor initialization. |

The child-spawning examples (`child_actors`, `address_cycle`) select
`children = unbounded`.
This enables `spawn_child` without a finite limit.
Its error is `Infallible`.
Those examples destructure `Ok` without panicking.

An address cycle does not create lifecycle ownership.
Cyclic calls can still wait forever.
