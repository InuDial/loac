//! Scheduled reply scaling benchmarks.
//!
//! `complete_all` releases an entire pending reply set.
//! `single_wake_to_target_poll` measures one notification.
//! The mailbox case measures handoff under queued traffic.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use loac::{
    Actor, ActorOwner, ActorRef, ActorScope, Cx, ExitReason, Handler, Message, Response, Shutdown,
    SpawnOptions, TryCallErrorKind, spawn_with,
};
use tokio::sync::{mpsc, oneshot};

const ACTIVE_COUNTS: [usize; 3] = [1, 32, 256];
const MAILBOX_BACKLOG: usize = 32;

struct ReplyActor;

#[loac::actor(mailbox = dynamic, interleaved = dynamic)]
impl Actor for ReplyActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PendingReply {
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<PendingReply> for ReplyActor {
    async fn handle(message: PendingReply, _cx: Cx<'_, Self>) {
        let _ = message.started.send(());
        let _ = message.release.await;
    }
}

struct WakeCommand {
    started: Instant,
    completed: oneshot::Sender<Duration>,
}

struct WakeProbe {
    started: Option<oneshot::Sender<()>>,
    commands: mpsc::Receiver<WakeCommand>,
}

impl Future for WakeProbe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        loop {
            match self.commands.poll_recv(task) {
                Poll::Ready(Some(WakeCommand { started, completed })) => {
                    let _ = completed.send(started.elapsed());
                }
                Poll::Ready(None) => panic!("a measured wake probe lost its controller"),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Probe(WakeProbe);

impl Handler<Probe> for ReplyActor {
    fn handle(message: Probe, _cx: Cx<'_, Self>) -> impl Future<Output = ()> + Send + '_ {
        message.0
    }
}

#[derive(Message)]
#[message(reply = ())]
struct MailboxBacklog;

impl Handler<MailboxBacklog> for ReplyActor {
    async fn handle(_message: MailboxBacklog, _cx: Cx<'_, Self>) {}
}

#[derive(Message)]
#[message(reply = ())]
struct MailboxTurnTrigger {
    commands: mpsc::Sender<WakeCommand>,
    completed: oneshot::Sender<Duration>,
}

impl Handler<MailboxTurnTrigger> for ReplyActor {
    async fn handle(message: MailboxTurnTrigger, _cx: Cx<'_, Self>) {
        message
            .commands
            .try_send(WakeCommand {
                started: Instant::now(),
                completed: message.completed,
            })
            .expect("the selected probe has one empty command slot");
    }
}

#[derive(Message)]
#[message(reply = ())]
struct StageMailboxBacklog {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<StageMailboxBacklog> for ReplyActor {
    async fn handle(message: StageMailboxBacklog, mut cx: Cx<'_, Self>) {
        let _guard = cx.exclusive();
        let _ = message.entered.send(());
        message
            .release
            .await
            .expect("the benchmark releases the staging barrier");
    }
}

fn spawn_benchmark_actor(active: usize) -> ActorOwner<ReplyActor> {
    let active = NonZeroUsize::new(active).expect("active reply counts are non-zero");
    spawn_with::<ReplyActor>(
        (),
        SpawnOptions::<ReplyActor>::default()
            .with_mailbox_capacity(active)
            .with_max_in_flight(active),
    )
}

async fn install_pending(
    actor: &ActorRef<ReplyActor>,
    count: usize,
) -> (Vec<oneshot::Sender<()>>, Vec<Response<()>>) {
    let mut started = Vec::with_capacity(count);
    let mut releases = Vec::with_capacity(count);
    let mut responses = Vec::with_capacity(count);

    for _ in 0..count {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        responses.push(
            actor
                .try_call(PendingReply {
                    started: started_tx,
                    release: release_rx,
                })
                .expect("the benchmark mailbox has capacity"),
        );
        started.push(started_rx);
        releases.push(release_tx);
    }
    for started in started {
        started.await.expect("each reply reaches its first poll");
    }

    (releases, responses)
}

async fn install_probes(
    actor: &ActorRef<ReplyActor>,
    count: usize,
) -> (Vec<mpsc::Sender<WakeCommand>>, Vec<Response<()>>) {
    let mut started = Vec::with_capacity(count);
    let mut commands = Vec::with_capacity(count);
    let mut responses = Vec::with_capacity(count);

    for _ in 0..count {
        let (started_tx, started_rx) = oneshot::channel();
        let (commands_tx, commands_rx) = mpsc::channel(1);
        responses.push(
            actor
                .try_call(Probe(WakeProbe {
                    started: Some(started_tx),
                    commands: commands_rx,
                }))
                .expect("the benchmark mailbox has capacity"),
        );
        started.push(started_rx);
        commands.push(commands_tx);
    }
    for started in started {
        started.await.expect("each probe reaches its first poll");
    }

    (commands, responses)
}

async fn measure_complete_all(iters: u64, active: usize) -> Duration {
    let owner = spawn_benchmark_actor(active);
    let actor = &owner;
    let mut measured = Duration::ZERO;

    for _ in 0..iters {
        let (releases, responses) = install_pending(actor, active).await;
        let started = Instant::now();
        for release in releases {
            release.send(()).expect("the reply remains scheduled");
        }
        for response in responses {
            response.await.expect("the benchmark actor stays alive");
        }
        measured += started.elapsed();
    }

    assert_eq!(
        owner.shutdown(Shutdown::Kill).await.reason(),
        ExitReason::Killed
    );
    measured
}

async fn measure_single_wake(iters: u64, active: usize) -> Duration {
    let owner = spawn_benchmark_actor(active);
    let actor = &owner;
    let (commands, responses) = install_probes(actor, active).await;
    let mut measured = Duration::ZERO;

    for iteration in 0..iters {
        let index = iteration as usize % active;
        let (completed_tx, completed_rx) = oneshot::channel();
        commands[index]
            .try_send(WakeCommand {
                started: Instant::now(),
                completed: completed_tx,
            })
            .expect("the selected probe remains scheduled");
        measured += completed_rx.await.expect("the probe records its latency");
    }

    assert_eq!(
        owner.shutdown(Shutdown::Kill).await.reason(),
        ExitReason::Killed
    );
    drop(commands);
    drop(responses);
    measured
}

async fn measure_mailbox_handoff(iters: u64, backlog: usize) -> Duration {
    let capacity = NonZeroUsize::new(backlog).expect("the backlog is non-zero");
    let owner = spawn_with::<ReplyActor>(
        (),
        SpawnOptions::<ReplyActor>::default()
            .with_mailbox_capacity(capacity)
            .with_max_in_flight(NonZeroUsize::new(2).unwrap()),
    );
    let actor = &owner;
    let (mut commands, probes) = install_probes(actor, 1).await;
    let commands = commands.pop().expect("one probe was installed");
    let mut measured = Duration::ZERO;

    for _ in 0..iters {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let barrier = actor
            .try_call(StageMailboxBacklog {
                entered: entered_tx,
                release: release_rx,
            })
            .expect("the staging barrier is admitted");
        entered_rx
            .await
            .expect("the barrier reaches its first poll");

        let (completed_tx, completed_rx) = oneshot::channel();
        let trigger = actor
            .try_call(MailboxTurnTrigger {
                commands: commands.clone(),
                completed: completed_tx,
            })
            .expect("the mailbox trigger is admitted");
        let queued: Vec<_> = (1..backlog)
            .map(|_| {
                actor
                    .try_call(MailboxBacklog)
                    .expect("the configured backlog is admitted")
            })
            .collect();
        assert_eq!(
            actor
                .try_call(MailboxBacklog)
                .expect_err("the mailbox is full")
                .kind(),
            TryCallErrorKind::Full
        );

        release_tx.send(()).expect("the barrier remains scheduled");
        measured += completed_rx.await.expect("the probe records its latency");
        barrier.await.expect("the barrier completes");
        trigger.await.expect("the trigger completes");
        for response in queued {
            response.await.expect("the mailbox backlog drains");
        }
    }

    assert_eq!(
        owner.shutdown(Shutdown::Kill).await.reason(),
        ExitReason::Killed
    );
    drop(commands);
    drop(probes);
    measured
}

fn reply_execution(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("the benchmark runtime builds");
    let mut group = criterion.benchmark_group("reply_execution");

    for active in ACTIVE_COUNTS {
        group.throughput(Throughput::Elements(active as u64));
        group.bench_with_input(
            BenchmarkId::new("complete_all", active),
            &active,
            |bencher, &active| {
                bencher
                    .to_async(&runtime)
                    .iter_custom(move |iters| measure_complete_all(iters, active));
            },
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_wake_to_target_poll", active),
            &active,
            |bencher, &active| {
                bencher
                    .to_async(&runtime)
                    .iter_custom(move |iters| measure_single_wake(iters, active));
            },
        );
    }

    group.bench_with_input(
        BenchmarkId::new("mailbox_turn_to_target_poll", MAILBOX_BACKLOG),
        &MAILBOX_BACKLOG,
        |bencher, &backlog| {
            bencher
                .to_async(&runtime)
                .iter_custom(move |iters| measure_mailbox_handoff(iters, backlog));
        },
    );
    group.finish();
}

criterion_group!(benches, reply_execution);
criterion_main!(benches);
