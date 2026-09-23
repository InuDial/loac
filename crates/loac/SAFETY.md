# `Cx` Safety Invariants

`Cx` safety depends on the following invariants.

## Storage lifetime

- [`ActorAccess`](src/runtime/access.rs) owns one pinned `ActorCell`.
- `ActorCell` holds the actor and scope state.
- Every `CxTarget` points into that pinned cell.
- `RunningActor` owns its scheduler before actor storage.
- Rust therefore drops the scheduler first.
- Kill and failure also clear the scheduler first.

Scheduled futures cannot outlive their actor storage.

## Borrow exclusivity

- The actor task owns every mutable cell access.
- It polls only one actor operation simultaneously.
- Dispatch creates `Cx` from pinned storage directly.
- Dispatch never creates a mutable actor reference.
- [`Cx::with`](src/access.rs) creates temporary mutable borrows.
- Its higher-ranked closure prevents those borrows escaping.
- Actor and scope state occupy separate `UnsafeCell` fields.
- `Cx::myself` only borrows the immutable actor address.

No safe mutable reference survives `Cx::with`.

## Lifetime erasure

`Cx::new` borrows `ActorAccess`, not its actor.
It returns `Cx` and `ScopedWake` together.
Both values carry the same dispatch lifetime.
The handler future retains that witness.

[`ScheduledFuture::scoped`](src/scheduling.rs) performs one lifetime transmute.
The scheduler then owns both future and wake state.
The erased lifetime never enters a public type.

Future destructors may call `Cx::with`.
Scheduler teardown keeps actor storage alive during destruction.

## Directed wakes

Each reply owns one [`ReplyWake`](src/scheduling/wake.rs).
A wake marks that reply ready, pushes its key once, and wakes the actor task.
The drain polls exactly the pushed keys.
Stale keys fail the slot map version check.

## Scheduler states

| Event | Resulting invariant |
| --- | --- |
| Dispatch queues work | The scheduler owns future and wake state |
| Poll returns `Pending` | No temporary actor borrow remains |
| Poll acquires a lease | The lease slot records that reply key |
| Leased poll returns `Pending` | Other scheduled actor work pauses |
| Guard drops | The lease slot clears |
| Poll returns `Ready` | Queue removes the item before dropping |
| Poll panics | Queue retains the item for cleanup |
| Kill or failure commits | Scheduler clears before actor storage |

Lease acquisition occurs only during scheduler polling.
Queue insertion therefore always attaches an unheld lease.
Removal force-releases any forgotten holder.

Graceful `on_shutdown` may run during a held lease.
It runs between polls after temporary borrows end.
It may observe partially updated actor state.

## Thread movement

`Cx` implements `Send`, but not `Sync`.
The whole actor task may move between threads.
Only that task dereferences each `CxTarget`.
No concurrent target access occurs.

## Unsafe boundaries

| Source | Required obligation |
| --- | --- |
| [`runtime/access.rs`](src/runtime/access.rs) | Target remains live and uniquely accessed |
| [`access.rs`](src/access.rs) | Actor-aware polls remain serialized |
| [`scheduling.rs`](src/scheduling.rs) | Scheduler drops before actor storage |
| [`queue.rs`](src/scheduling/queue.rs) | The leased reply is the only polled reply |
