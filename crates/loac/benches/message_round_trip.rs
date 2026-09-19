//! Message round-trip benchmarks across reply concurrency profiles.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use loac::prelude::*;

struct SerialActor;

#[loac::actor(mailbox = 1)]
impl Actor for SerialActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct FixedActor;

#[loac::actor(mailbox = 1, interleaved = 1)]
impl Actor for FixedActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct UnboundedActor;

#[loac::actor(mailbox = 1, interleaved = unbounded)]
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

reply_handler!(SerialActor);
reply_handler!(FixedActor);
reply_handler!(UnboundedActor);

#[derive(Message)]
#[message(reply = u64)]
struct Exclusive;

impl Handler<Exclusive> for FixedActor {
    async fn handle(_message: Exclusive, mut cx: Cx<'_, Self>) -> u64 {
        let _guard = cx.exclusive();
        1
    }
}

fn message_round_trip(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the benchmark runtime builds");
    let serial = runtime.block_on(async { loac::spawn::<SerialActor>(()) });
    let fixed = runtime.block_on(async { loac::spawn::<FixedActor>(()) });
    let unbounded = runtime.block_on(async { loac::spawn::<UnboundedActor>(()) });

    let mut group = criterion.benchmark_group("message_round_trip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("serial", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(serial.call(Reply).await.expect("the actor stays alive")) });
    });
    group.bench_function("fixed", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(fixed.call(Reply).await.expect("the actor stays alive")) });
    });
    group.bench_function("unbounded", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(unbounded.call(Reply).await.expect("the actor stays alive"))
        });
    });
    group.bench_function("exclusive", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(fixed.call(Exclusive).await.expect("the actor stays alive"))
        });
    });
    group.finish();

    assert_eq!(
        runtime
            .block_on(serial.shutdown(loac::Shutdown::Kill))
            .reason(),
        loac::ExitReason::Killed
    );
    assert_eq!(
        runtime
            .block_on(fixed.shutdown(loac::Shutdown::Kill))
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
