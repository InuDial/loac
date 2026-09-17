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
- [`Cx::with`](src/access.rs) creates temporary mutable borrows.
- Its higher-ranked closure prevents those borrows escaping.
- Actor and scope state occupy separate `UnsafeCell` fields.
- `Cx::myself` only borrows the immutable actor address.

No safe mutable reference survives `Cx::with`.

## Lifetime erasure

`Cx::new` returns `Cx` and `ScopedLease` together.
Both values carry the same dispatch lifetime.
`CxReply` and `CxStream` retain that witness.

[`ScheduledFuture::scoped`](src/scheduling.rs) performs one lifetime transmute.
It boxes the completed scheduling wrapper first.
The scheduler then owns both future and lease.
The erased lifetime never enters a public type.

Future destructors may call `Cx::with`.
Scheduler teardown keeps actor storage alive during destruction.

## Scheduler states

| Event | Resulting invariant |
| --- | --- |
| Dispatch queues work | Handler construction remains deferred |
| Poll returns `Pending` | No temporary actor borrow remains |
| Poll acquires a lease | That item remains at the queue front |
| Leased poll returns `Pending` | Other scheduled actor work pauses |
| Guard drops | Queue rotation may resume |
| Poll returns `Ready` | Queue removes the item before dropping |
| Poll panics | Queue retains the item for cleanup |
| Kill or failure commits | Scheduler clears before actor storage |

Lease acquisition occurs only during scheduler polling.
Queue insertion therefore always receives an unheld lease.

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
| [`queue.rs`](src/scheduling/queue.rs) | Leased work remains at the front |
