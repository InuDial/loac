use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use super::*;
use crate::{Actor, ActorConfig, ActorScope, Shutdown, mailbox::ActorInner};

struct TestActor;

#[crate::actor(mailbox, mailbox_capacity = 1)]
impl Actor for TestActor {
    type SpawnArgs = ();

    async fn init(_args: (), _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn test_actor_inner() -> Arc<ActorInner<TestActor>> {
    let options = <TestActor as ActorConfig>::Options::default();
    ActorInner::open(&options).0
}

fn poll_queue(queue: &mut Queue, control: &Control, waker: &Waker) -> ReplyPoll {
    let mut task = Context::from_waker(waker);
    queue.poll(control, Mode::Running, &mut task)
}

struct PollCounter {
    polls: Arc<AtomicUsize>,
    completes: bool,
}

impl Future for PollCounter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.completes {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct SelfWakeOnce {
    polls: Arc<AtomicUsize>,
    woken: bool,
}

impl Future for SelfWakeOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.woken {
            return Poll::Ready(());
        }
        self.woken = true;
        task.waker().wake_by_ref();
        Poll::Pending
    }
}

struct CaptureWaker {
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for CaptureWaker {
    type Output = ();

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<()> {
        *self.waker.lock().unwrap() = Some(task.waker().clone());
        Poll::Pending
    }
}

struct KillOnPoll {
    actor: Arc<ActorInner<TestActor>>,
    polls: Arc<AtomicUsize>,
}

impl Future for KillOnPoll {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.actor.control.request(Shutdown::Kill);
        Poll::Pending
    }
}

struct ReadyWithPanickingDrop {
    drops: Arc<AtomicUsize>,
    dropped_while_unwinding: Arc<AtomicBool>,
}

impl Future for ReadyWithPanickingDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

impl Drop for ReadyWithPanickingDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.dropped_while_unwinding
            .store(std::thread::panicking(), Ordering::SeqCst);
        panic!("intentional future drop panic");
    }
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("intentional actor task wake panic");
    }

    fn wake_by_ref(self: &Arc<Self>) {
        panic!("intentional actor task wake panic");
    }
}

#[test]
fn empty_poll_is_pending() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
    assert!(queue.is_empty());
}

#[test]
fn push_marks_only_the_first_poll_ready() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let polls = Arc::new(AtomicUsize::new(0));
    queue.schedule_test(PollCounter {
        polls: Arc::clone(&polls),
        completes: false,
    });

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[test]
fn wake_pushes_one_key_per_ready_episode() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let retained = Arc::new(Mutex::new(None));
    queue.schedule_test(CaptureWaker {
        waker: Arc::clone(&retained),
    });
    let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::Pending
    );
    let retained = retained
        .lock()
        .unwrap()
        .clone()
        .expect("the future captured its waker");

    retained.wake_by_ref();
    retained.wake_by_ref();

    // The first wake consumes the registered task waker.
    // Both calls collapse into one queued key.
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(queue.ready.len(), 1);

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::Pending
    );
    assert_eq!(queue.ready.len(), 0);
}

#[test]
fn budget_truncates_then_self_wakes() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let polls = Arc::new(AtomicUsize::new(0));
    for _ in 0..(ACTIVE_POLL_BUDGET + 4) {
        queue.schedule_test(PollCounter {
            polls: Arc::clone(&polls),
            completes: false,
        });
    }
    let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::BudgetExhausted
    );
    assert_eq!(polls.load(Ordering::SeqCst), ACTIVE_POLL_BUDGET);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::Pending
    );
    assert_eq!(polls.load(Ordering::SeqCst), ACTIVE_POLL_BUDGET + 4);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
}

#[test]
fn wake_during_poll_is_drained_in_the_same_turn() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let polls = Arc::new(AtomicUsize::new(0));
    queue.schedule_test(SelfWakeOnce {
        polls: Arc::clone(&polls),
        woken: false,
    });

    // The self-wake pushes a fresh key that the same drain consumes.
    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Progress
    );
    assert!(queue.is_empty());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[test]
fn stale_keys_are_skipped() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let key = queue.schedule_test(async {});

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Progress
    );
    queue.ready.push(key);

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
}

#[test]
fn a_lease_pauses_every_other_reply() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let leased_polls = Arc::new(AtomicUsize::new(0));
    queue.schedule_leased(PollCounter {
        polls: Arc::clone(&leased_polls),
        completes: false,
    });
    let other_polls = Arc::new(AtomicUsize::new(0));
    queue.schedule_test(PollCounter {
        polls: Arc::clone(&other_polls),
        completes: true,
    });
    assert!(queue.is_leased());

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Leased
    );
    assert_eq!(leased_polls.load(Ordering::SeqCst), 1);
    assert_eq!(other_polls.load(Ordering::SeqCst), 0);

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
    assert_eq!(other_polls.load(Ordering::SeqCst), 0);

    queue.lease.force_release();
    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Progress
    );
    assert_eq!(other_polls.load(Ordering::SeqCst), 1);
    // The released reply remains parked until it is woken again.
    assert_eq!(queue.len(), 1);
}

#[test]
fn completing_the_leased_reply_releases_the_slot() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    queue.schedule_leased(async {});

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Progress
    );
    assert!(!queue.is_leased());
    assert!(queue.is_empty());
}

#[test]
fn kill_committed_by_one_reply_stops_the_drain() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let first_polls = Arc::new(AtomicUsize::new(0));
    let second_polls = Arc::new(AtomicUsize::new(0));
    queue.schedule_test(KillOnPoll {
        actor: Arc::clone(&actor),
        polls: Arc::clone(&first_polls),
    });
    queue.schedule_test(PollCounter {
        polls: Arc::clone(&second_polls),
        completes: true,
    });

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Progress
    );
    assert_eq!(first_polls.load(Ordering::SeqCst), 1);
    assert_eq!(second_polls.load(Ordering::SeqCst), 0);
    assert_eq!(actor.control.mode(), Mode::Killing);
}

#[test]
fn wake_contains_actor_task_waker_panic() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let retained = Arc::new(Mutex::new(None));
    queue.schedule_test(CaptureWaker {
        waker: Arc::clone(&retained),
    });
    let waker = Waker::from(Arc::new(PanicWake));

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::Pending
    );
    retained
        .lock()
        .unwrap()
        .as_ref()
        .expect("the future captured its waker")
        .wake_by_ref();
}

#[test]
fn budget_continuation_contains_actor_task_waker_panic() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    for _ in 0..=ACTIVE_POLL_BUDGET {
        queue.schedule_test(std::future::pending::<()>());
    }
    let waker = Waker::from(Arc::new(PanicWake));

    assert_eq!(
        poll_queue(&mut queue, &actor.control, &waker),
        ReplyPoll::BudgetExhausted
    );
}

#[test]
fn late_wake_after_clear_is_a_no_op() {
    let actor = test_actor_inner();
    let mut queue = Queue::new();
    let retained = Arc::new(Mutex::new(None));
    queue.schedule_test(CaptureWaker {
        waker: Arc::clone(&retained),
    });

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
    let retained = retained
        .lock()
        .unwrap()
        .clone()
        .expect("the future captured its waker");

    queue.clear(&actor.control);
    retained.wake_by_ref();

    assert_eq!(
        poll_queue(&mut queue, &actor.control, Waker::noop()),
        ReplyPoll::Pending
    );
}

#[test]
fn clear_releases_the_lease_and_contains_drops() {
    let actor = test_actor_inner();
    let leased_drops = Arc::new(AtomicUsize::new(0));
    let reply_drops = Arc::new(AtomicUsize::new(0));
    let dropped_while_unwinding = Arc::new(AtomicBool::new(false));
    let mut queue = Queue::new();

    queue.schedule_leased(ReadyWithPanickingDrop {
        drops: Arc::clone(&leased_drops),
        dropped_while_unwinding: Arc::clone(&dropped_while_unwinding),
    });
    queue.schedule_test(ReadyWithPanickingDrop {
        drops: Arc::clone(&reply_drops),
        dropped_while_unwinding: Arc::clone(&dropped_while_unwinding),
    });

    queue.clear(&actor.control);

    assert_eq!(leased_drops.load(Ordering::SeqCst), 1);
    assert_eq!(reply_drops.load(Ordering::SeqCst), 1);
    assert!(!dropped_while_unwinding.load(Ordering::SeqCst));
    assert!(!queue.is_leased());
    assert!(queue.is_empty());
}

// Automatic scheduler teardown has no lifecycle handle.
// Each future still needs an independent unwind boundary.
#[test]
fn queue_drop_contains_each_future_panic() {
    let drops = Arc::new(AtomicUsize::new(0));
    let dropped_while_unwinding = Arc::new(AtomicBool::new(false));
    let mut queue = Queue::new();
    for _ in 0..2 {
        queue.schedule_test(ReadyWithPanickingDrop {
            drops: Arc::clone(&drops),
            dropped_while_unwinding: Arc::clone(&dropped_while_unwinding),
        });
    }

    drop(queue);

    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(!dropped_while_unwinding.load(Ordering::SeqCst));
}
