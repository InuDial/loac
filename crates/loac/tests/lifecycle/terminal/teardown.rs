use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use loac::{
    Actor, ActorScope, CallError, Cx, ExitReason, Handler, Message, StreamHandler, StreamOut,
    SubtreeStatus, Writer, actor,
};
use tokio::sync::oneshot;

struct PendingInit;

#[actor(mailbox)]
impl Actor for PendingInit {
    type SpawnArgs = oneshot::Sender<()>;

    async fn init(entered: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        let _ = entered.send(());
        std::future::pending::<Self>().await
    }
}

#[derive(Message)]
#[message(reply = ())]
struct QueuedDrop {
    dropped: Arc<AtomicBool>,
    dropped_while_unwinding: Arc<AtomicBool>,
    panic: bool,
}

impl Drop for QueuedDrop {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.dropped_while_unwinding
            .store(std::thread::panicking(), Ordering::SeqCst);
        assert!(!self.panic, "intentional queued message drop panic");
    }
}

impl Handler<QueuedDrop> for PendingInit {
    async fn handle(_message: QueuedDrop, _cx: Cx<'_, Self>) {
        unreachable!("pending initialization prevents dispatch");
    }
}

// Executor teardown must use the accepted-envelope cleanup boundary.
// Earlier panics cannot skip later messages or unwind their destructors.
// Synchronous abort also loses subtree confirmation.
#[test]
fn executor_teardown_discards_each_accepted_message() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let call_dropped = Arc::new(AtomicBool::new(false));
    let send_dropped = Arc::new(AtomicBool::new(false));
    let tail_dropped = Arc::new(AtomicBool::new(false));
    let tail_unwinding = Arc::new(AtomicBool::new(false));

    let (owner, response) = runtime.block_on(async {
        let owner = loac::spawn::<PendingInit>(entered_tx);
        entered_rx.await.unwrap();
        let actor = owner.actor_ref();
        let response = actor
            .try_call(QueuedDrop {
                dropped: Arc::clone(&call_dropped),
                dropped_while_unwinding: Arc::new(AtomicBool::new(false)),
                panic: true,
            })
            .unwrap();
        actor
            .try_send(QueuedDrop {
                dropped: Arc::clone(&send_dropped),
                dropped_while_unwinding: Arc::new(AtomicBool::new(false)),
                panic: true,
            })
            .unwrap();
        actor
            .try_send(QueuedDrop {
                dropped: Arc::clone(&tail_dropped),
                dropped_while_unwinding: Arc::clone(&tail_unwinding),
                panic: false,
            })
            .unwrap();
        (owner, response)
    });
    let actor = owner.actor_ref();

    drop(runtime);

    assert!(call_dropped.load(Ordering::SeqCst));
    assert!(send_dropped.load(Ordering::SeqCst));
    assert!(tail_dropped.load(Ordering::SeqCst));
    assert!(!tail_unwinding.load(Ordering::SeqCst));
    let mut response = Box::pin(response);
    let mut task = Context::from_waker(Waker::noop());
    assert_eq!(
        response.as_mut().poll(&mut task),
        Poll::Ready(Err(CallError::BeforeDispatch(ExitReason::Aborted)))
    );
    let status = actor
        .exit_status()
        .expect("actor teardown publishes a status");
    assert_eq!(status.reason(), ExitReason::Aborted);
    assert_eq!(status.subtree(), SubtreeStatus::Unconfirmed);
    drop(owner);
}

struct PendingReplyActor;

#[actor(mailbox = 2, interleaved = 2)]
impl Actor for PendingReplyActor {
    type SpawnArgs = ();

    async fn init(_: (), _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PendingReplyDrop {
    entered: Option<oneshot::Sender<()>>,
    drops: Arc<AtomicUsize>,
    dropped_while_unwinding: Arc<AtomicBool>,
}

impl std::future::Future for PendingReplyDrop {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<()> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        Poll::Pending
    }
}

impl Drop for PendingReplyDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.dropped_while_unwinding
            .store(std::thread::panicking(), Ordering::SeqCst);
        panic!("intentional reply future drop panic");
    }
}

impl Handler<PendingReplyDrop> for PendingReplyActor {
    async fn handle(message: PendingReplyDrop, _cx: Cx<'_, Self>) {
        message.await
    }
}

// Executor teardown drops scheduler entries without lifecycle access.
// Each entry still needs its own unwind boundary.
#[test]
fn executor_teardown_contains_each_reply_drop_panic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let dropped_while_unwinding = Arc::new(AtomicBool::new(false));
    let (first_entered_tx, first_entered_rx) = oneshot::channel();
    let (second_entered_tx, second_entered_rx) = oneshot::channel();

    let (owner, first, second) = runtime.block_on(async {
        let owner = loac::spawn::<PendingReplyActor>(());
        let actor = owner.actor_ref();
        let first = actor
            .try_call(PendingReplyDrop {
                entered: Some(first_entered_tx),
                drops: Arc::clone(&drops),
                dropped_while_unwinding: Arc::clone(&dropped_while_unwinding),
            })
            .unwrap();
        let second = actor
            .try_call(PendingReplyDrop {
                entered: Some(second_entered_tx),
                drops: Arc::clone(&drops),
                dropped_while_unwinding: Arc::clone(&dropped_while_unwinding),
            })
            .unwrap();
        first_entered_rx.await.unwrap();
        second_entered_rx.await.unwrap();
        (owner, first, second)
    });
    let actor = owner.actor_ref();

    drop(runtime);

    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(!dropped_while_unwinding.load(Ordering::SeqCst));
    for mut response in [Box::pin(first), Box::pin(second)] {
        let mut task = Context::from_waker(Waker::noop());
        assert_eq!(
            response.as_mut().poll(&mut task),
            Poll::Ready(Err(CallError::DuringDispatch(ExitReason::Aborted)))
        );
    }
    let status = actor
        .exit_status()
        .expect("actor teardown publishes a status");
    assert_eq!(status.reason(), ExitReason::Aborted);
    assert_eq!(status.subtree(), SubtreeStatus::Unconfirmed);
    drop(owner);
}

struct CxDropActor {
    order: Arc<AtomicUsize>,
    touched: bool,
}

impl Drop for CxDropActor {
    fn drop(&mut self) {
        let value = if self.touched { 2 } else { usize::MAX };
        self.order.store(value, Ordering::SeqCst);
    }
}

#[actor(mailbox, interleaved)]
impl Actor for CxDropActor {
    type SpawnArgs = Arc<AtomicUsize>;

    async fn init(order: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self {
            order,
            touched: false,
        }
    }
}

#[derive(Message)]
#[message(reply = ())]
struct PendingCxDrop(oneshot::Sender<()>);

struct CxDropFuture<'a> {
    cx: Cx<'a, CxDropActor>,
    entered: Option<oneshot::Sender<()>>,
}

impl std::future::Future for CxDropFuture<'_> {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, _task: &mut Context<'_>) -> Poll<()> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        Poll::Pending
    }
}

impl Drop for CxDropFuture<'_> {
    fn drop(&mut self) {
        self.cx.with(|actor, _scope| {
            actor.touched = true;
            actor.order.store(1, Ordering::SeqCst);
        });
    }
}

impl Handler<PendingCxDrop> for CxDropActor {
    fn handle<'a>(
        message: PendingCxDrop,
        cx: Cx<'a, Self>,
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        CxDropFuture {
            cx,
            entered: Some(message.0),
        }
    }
}

#[test]
fn executor_teardown_drops_cx_futures_before_actor_storage() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let order = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();

    let (owner, response) = runtime.block_on(async {
        let owner = loac::spawn::<CxDropActor>(Arc::clone(&order));
        let response = owner.try_call(PendingCxDrop(entered_tx)).unwrap();
        entered_rx.await.unwrap();
        (owner, response)
    });

    drop(runtime);

    assert_eq!(order.load(Ordering::SeqCst), 2);
    drop((owner, response));
}

struct CxStreamDropActor {
    order: Arc<AtomicUsize>,
    touched: bool,
}

impl Drop for CxStreamDropActor {
    fn drop(&mut self) {
        let value = if self.touched { 2 } else { usize::MAX };
        self.order.store(value, Ordering::SeqCst);
    }
}

#[actor(mailbox, interleaved)]
impl Actor for CxStreamDropActor {
    type SpawnArgs = Arc<AtomicUsize>;

    async fn init(order: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self {
            order,
            touched: false,
        }
    }
}

#[derive(Message)]
#[message(stream = u8, reply = ())]
struct PendingCxStreamDrop(oneshot::Sender<()>);

struct CxStreamDropState<'a, W> {
    cx: Cx<'a, CxStreamDropActor>,
    _out: StreamOut<'a, W>,
}

impl<W> Drop for CxStreamDropState<'_, W> {
    fn drop(&mut self) {
        self.cx.with(|actor, _scope| {
            actor.touched = true;
            actor.order.store(1, Ordering::SeqCst);
        });
    }
}

impl StreamHandler<PendingCxStreamDrop> for CxStreamDropActor {
    async fn handle<'a, W>(message: PendingCxStreamDrop, out: StreamOut<'a, W>, cx: Cx<'a, Self>)
    where
        W: Writer<u8> + Send + 'a,
    {
        let _state = CxStreamDropState { cx, _out: out };
        let _ = message.0.send(());
        std::future::pending().await
    }
}

#[test]
fn executor_teardown_drops_default_stream_before_actor_storage() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let order = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();

    let (owner, response) = runtime.block_on(async {
        let owner = loac::spawn::<CxStreamDropActor>(Arc::clone(&order));
        let response = owner.try_call(PendingCxStreamDrop(entered_tx)).unwrap();
        entered_rx.await.unwrap();
        (owner, response)
    });

    drop(runtime);

    assert_eq!(order.load(Ordering::SeqCst), 2);
    drop((owner, response));
}

#[test]
fn executor_teardown_drops_call_to_stream_before_actor_storage() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let order = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();

    let (owner, response) = runtime.block_on(async {
        let owner = loac::spawn::<CxStreamDropActor>(Arc::clone(&order));
        let actor = owner.actor_ref();
        let (item_tx, _item_rx) = tokio::sync::mpsc::channel(1);
        let response = tokio::spawn(async move {
            actor
                .call_to(PendingCxStreamDrop(entered_tx), item_tx)
                .await
        });
        entered_rx.await.unwrap();
        (owner, response)
    });

    drop(runtime);

    assert_eq!(order.load(Ordering::SeqCst), 2);
    drop((owner, response));
}
