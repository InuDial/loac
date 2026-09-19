//! Steady-state allocation counts for scheduled replies.
//!
//! Measurement includes admission, response transport, and scheduling.
//! Runtime creation and actor spawn stay outside each region.

use std::hint::black_box;

use allocation_counter::{AllocationInfo, measure};
use loac::{ActorRef, prelude::*};

const WARMUP_CALLS: usize = 64;
const MEASURED_CALLS: usize = 10_000;

struct ReplyActor;

#[loac::actor(mailbox = 1, interleaved = 1)]
impl Actor for ReplyActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Reply;

impl Handler<Reply> for ReplyActor {
    async fn handle(_message: Reply, _cx: Cx<'_, Self>) -> u64 {
        1
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Exclusive;

impl Handler<Exclusive> for ReplyActor {
    async fn handle(_message: Exclusive, mut cx: Cx<'_, Self>) -> u64 {
        let _guard = cx.exclusive();
        1
    }
}

fn run_replies(runtime: &tokio::runtime::Runtime, actor: &ActorRef<ReplyActor>, calls: usize) {
    runtime.block_on(async {
        for _ in 0..calls {
            black_box(actor.call(Reply).await.expect("the actor stays alive"));
        }
    });
}

fn run_exclusive(runtime: &tokio::runtime::Runtime, actor: &ActorRef<ReplyActor>, calls: usize) {
    runtime.block_on(async {
        for _ in 0..calls {
            black_box(actor.call(Exclusive).await.expect("the actor stays alive"));
        }
    });
}

fn measure_calls(run: impl FnOnce()) -> AllocationInfo {
    measure(run)
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the benchmark runtime builds");
    let owner = runtime.block_on(async { loac::spawn::<ReplyActor>(()) });
    let actor = owner.actor_ref();

    run_replies(&runtime, &actor, WARMUP_CALLS);
    run_exclusive(&runtime, &actor, WARMUP_CALLS);

    let samples = [
        (
            "reply",
            measure_calls(|| run_replies(&runtime, &actor, MEASURED_CALLS)),
        ),
        (
            "exclusive",
            measure_calls(|| run_exclusive(&runtime, &actor, MEASURED_CALLS)),
        ),
    ];

    println!("reply allocations ({MEASURED_CALLS} calls per mode)");
    println!(
        "{:<12} {:>14} {:>12} {:>16} {:>12}",
        "mode", "allocations", "alloc/call", "bytes", "bytes/call"
    );
    for (mode, sample) in samples {
        println!(
            "{mode:<12} {:>14} {:>12.3} {:>16} {:>12.3}",
            sample.count_total,
            sample.count_total as f64 / MEASURED_CALLS as f64,
            sample.bytes_total,
            sample.bytes_total as f64 / MEASURED_CALLS as f64,
        );
    }

    assert_eq!(
        runtime
            .block_on(owner.shutdown(loac::Shutdown::Kill))
            .reason(),
        loac::ExitReason::Killed
    );
}
