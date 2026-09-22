//! Wake amplification under pending handler load.
//!
//! Each round keeps `active` handler futures suspended on their own channel.
//! One command wakes one probe.
//! The report shows median wake latency, mean handler polls, and the
//! marginal cost of one pending poll.
//! The `active = 1` row approximates the exact-wake lower bound.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use loac::{
    Actor, ActorRef, ActorScope, Cx, Handler, Message, Response, Shutdown, SpawnOptions, spawn_with,
};
use tokio::sync::{mpsc, oneshot};

/// Counts every poll of a benchmark probe future.
static POLLS: AtomicUsize = AtomicUsize::new(0);

const ACTIVE_COUNTS: [usize; 4] = [1, 8, 16, 32];
const WARMUP_ROUNDS: usize = 20;
const ROUNDS: usize = 200;

struct LoadActor;

#[loac::actor(mailbox, mailbox_capacity = dynamic, max_in_flight = dynamic)]
impl Actor for LoadActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

struct WakeCommand {
    started: Instant,
    completed: oneshot::Sender<Duration>,
}

struct CountingProbe {
    started: Option<oneshot::Sender<()>>,
    commands: mpsc::Receiver<WakeCommand>,
}

impl Future for CountingProbe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<()> {
        POLLS.fetch_add(1, Ordering::Relaxed);
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        loop {
            match self.commands.poll_recv(task) {
                Poll::Ready(Some(WakeCommand { started, completed })) => {
                    let _ = completed.send(started.elapsed());
                }
                Poll::Ready(None) => panic!("a measured probe lost its controller"),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct Probe(CountingProbe);

impl Handler<Probe> for LoadActor {
    fn handle(message: Probe, _cx: Cx<'_, Self>) -> impl Future<Output = ()> + Send + '_ {
        message.0
    }
}

async fn install_probes(
    actor: &ActorRef<LoadActor>,
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
                .try_call(Probe(CountingProbe {
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

async fn measure(active: usize) -> (Duration, usize) {
    let active_count = NonZeroUsize::new(active).expect("active counts are non-zero");
    let owner = spawn_with::<LoadActor>(
        (),
        SpawnOptions::<LoadActor>::default()
            .with_mailbox_capacity(active_count)
            .with_max_in_flight(active_count),
    );
    let actor = &owner;
    let (commands, responses) = install_probes(actor, active).await;
    let mut samples = Vec::with_capacity(ROUNDS);

    for iteration in 0..(WARMUP_ROUNDS + ROUNDS) {
        let index = iteration % active;
        let (completed_tx, completed_rx) = oneshot::channel();
        POLLS.store(0, Ordering::SeqCst);
        commands[index]
            .try_send(WakeCommand {
                started: Instant::now(),
                completed: completed_tx,
            })
            .expect("the selected probe remains scheduled");
        let elapsed = completed_rx.await.expect("the probe records its latency");
        if iteration >= WARMUP_ROUNDS {
            samples.push((elapsed, POLLS.load(Ordering::SeqCst)));
        }
    }

    let _ = owner.shutdown(Shutdown::Kill).await;
    drop(commands);
    drop(responses);

    let polls = samples.iter().map(|(_, polls)| *polls).sum::<usize>() / samples.len();
    samples.sort_by_key(|(elapsed, _)| *elapsed);
    (samples[samples.len() / 2].0, polls)
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the benchmark runtime builds");

    let results = runtime.block_on(async {
        let mut results = Vec::new();
        for active in ACTIVE_COUNTS {
            results.push((active, measure(active).await));
        }
        results
    });

    let (_, (base_elapsed, base_polls)) = results[0];
    println!("active   wake_us   polls   ns_per_poll");
    for (active, (elapsed, polls)) in &results {
        let extra_polls = polls.saturating_sub(base_polls);
        let ns_per_poll = if extra_polls == 0 {
            "-".to_string()
        } else {
            let extra_ns = elapsed.saturating_sub(base_elapsed).as_nanos() as f64;
            format!("{:.0}", extra_ns / extra_polls as f64)
        };
        println!(
            "{active:>6} {:>9.1} {polls:>7} {ns_per_poll:>12}",
            elapsed.as_secs_f64() * 1e6
        );
    }
}
