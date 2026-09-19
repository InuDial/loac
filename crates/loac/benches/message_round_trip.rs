//! Message round-trip benchmarks across reply concurrency profiles.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use loac::prelude::*;

struct DefaultActor;

#[loac::actor(mailbox, mailbox_capacity = 1)]
impl Actor for DefaultActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct SingleSlotActor;

#[loac::actor(mailbox, mailbox_capacity = 1, max_in_flight = 1)]
impl Actor for SingleSlotActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct UnboundedActor;

#[loac::actor(mailbox, mailbox_capacity = 1, max_in_flight = unbounded)]
impl Actor for UnboundedActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Reply;

macro_rules! reply_handler {
    ($actor:ty) => {
        impl Handler<Reply> for $actor {
            async fn handle(_message: Reply, _cx: Cx<'_, Self>) -> u64 {
                1
            }
        }
    };
}

reply_handler!(DefaultActor);
reply_handler!(SingleSlotActor);
reply_handler!(UnboundedActor);

#[derive(Message)]
#[message(reply = u64)]
struct Exclusive;

impl Handler<Exclusive> for SingleSlotActor {
    async fn handle(_message: Exclusive, mut cx: Cx<'_, Self>) -> u64 {
        let _guard = cx.exclusive();
        1
    }
}

fn message_round_trip(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the benchmark runtime builds");
    let default = runtime.block_on(async { loac::spawn::<DefaultActor>(()) });
    let single_slot = runtime.block_on(async { loac::spawn::<SingleSlotActor>(()) });
    let unbounded = runtime.block_on(async { loac::spawn::<UnboundedActor>(()) });

    let mut group = criterion.benchmark_group("message_round_trip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("default", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(default.call(Reply).await.expect("the actor stays alive"))
        });
    });
    group.bench_function("single_slot", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(
                single_slot
                    .call(Reply)
                    .await
                    .expect("the actor stays alive"),
            )
        });
    });
    group.bench_function("unbounded", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(unbounded.call(Reply).await.expect("the actor stays alive"))
        });
    });
    group.bench_function("exclusive", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(
                single_slot
                    .call(Exclusive)
                    .await
                    .expect("the actor stays alive"),
            )
        });
    });
    group.finish();

    assert_eq!(
        runtime
            .block_on(default.shutdown(loac::Shutdown::Kill))
            .reason(),
        loac::ExitReason::Killed
    );
    assert_eq!(
        runtime
            .block_on(single_slot.shutdown(loac::Shutdown::Kill))
            .reason(),
        loac::ExitReason::Killed
    );
    assert_eq!(
        runtime
            .block_on(unbounded.shutdown(loac::Shutdown::Kill))
            .reason(),
        loac::ExitReason::Killed
    );
}

criterion_group!(benches, message_round_trip);
criterion_main!(benches);
