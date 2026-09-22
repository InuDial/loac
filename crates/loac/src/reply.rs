//! Typed reply channels and runtime dispatch.
//!
//! Every handler returns one `Cx` future.
//! The actor task owns and polls that future.
//! The `max_in_flight` option only changes concurrency.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tokio::sync::{mpsc, oneshot};

use crate::{
    Actor, CallError, Handler, Message, StreamHandler, StreamOut,
    access::{Cx, ScopedWake},
    mailbox::DispatchReply,
    scheduling::{ActorScheduler, RuntimeScheduler, ScheduledFuture},
};

mod private {
    pub trait Sealed {}
}

/// Selects a message's reply channel shape.
///
/// [`Message`] derives this selection.
/// Custom implementations use [`SingleKind`] or [`StreamKind`].
pub trait ReplyKind: private::Sealed + Send + 'static {}

/// A message returning one value.
pub struct SingleKind;

impl private::Sealed for SingleKind {}
impl ReplyKind for SingleKind {}

/// A message returning items and one final value.
pub struct StreamKind;

impl private::Sealed for StreamKind {}
impl ReplyKind for StreamKind {}

// Connects a derived message shape to its handler trait.
// Generic address methods use this sealed runtime bridge.
pub(crate) trait DispatchMessage<A: Actor, M: Message>: ReplyKind {
    fn dispatch(
        message: M,
        cx: Cx<'_, A>,
        wake: ScopedWake<'_, A>,
        scheduler: &mut ActorScheduler<A>,
        reply: DispatchReply<'_, A, M::Reply>,
    );
}

impl<A, M> DispatchMessage<A, M> for SingleKind
where
    A: Handler<M>,
    M: Message<Kind = SingleKind>,
{
    fn dispatch(
        message: M,
        cx: Cx<'_, A>,
        wake: ScopedWake<'_, A>,
        scheduler: &mut ActorScheduler<A>,
        reply: DispatchReply<'_, A, M::Reply>,
    ) {
        let future = A::handle(message, cx);
        RuntimeScheduler::push(
            scheduler,
            ScheduledFuture::scoped(CompleteReply::new(future, reply.into_scheduled()), wake),
        );
    }
}

impl<A, M> DispatchMessage<A, M> for StreamKind
where
    A: StreamHandler<M>,
    M: StreamMessage,
{
    fn dispatch(
        message: M,
        cx: Cx<'_, A>,
        wake: ScopedWake<'_, A>,
        scheduler: &mut ActorScheduler<A>,
        reply: DispatchReply<'_, A, M::Reply>,
    ) {
        let (item_tx, item_rx) = mpsc::channel::<M::Item>(8);
        let (final_tx, final_rx) = oneshot::channel::<M::Final>();
        let out = StreamOut::new(item_tx);
        let future = A::handle(message, out, cx);

        reply.complete(StreamReply { item_rx, final_rx });
        RuntimeScheduler::push(
            scheduler,
            ScheduledFuture::scoped(FinishStream::new(future, final_tx), wake),
        );
    }
}

/// A caller handle for a streamed reply.
///
/// Read items with [`recv`](Self::recv) or [`items`](Self::items).
/// Then await the final value with [`finish`](Self::finish).
#[must_use = "a stream reply must be consumed or finished"]
#[derive(Debug)]
pub struct StreamReply<Item, Final> {
    item_rx: mpsc::Receiver<Item>,
    final_rx: oneshot::Receiver<Final>,
}

impl<Item, Final> StreamReply<Item, Final> {
    /// Receives the next item.
    pub async fn recv(&mut self) -> Option<Item> {
        self.item_rx.recv().await
    }

    /// Borrows the item channel as a stream.
    pub fn items(&mut self) -> Items<'_, Item> {
        Items {
            item_rx: &mut self.item_rx,
        }
    }

    /// Discards remaining items and returns the final value.
    pub async fn finish(mut self) -> Result<Final, CallError> {
        loop {
            tokio::select! {
                item = self.item_rx.recv() => {
                    if item.is_none() {
                        return self.final_rx.await.map_err(|_| CallError::ResponseLost);
                    }
                }
                result = &mut self.final_rx => {
                    while self.item_rx.try_recv().is_ok() {}
                    return result.map_err(|_| CallError::ResponseLost);
                }
            }
        }
    }
}

/// A borrowed view of streamed items.
#[derive(Debug)]
pub struct Items<'a, Item> {
    item_rx: &'a mut mpsc::Receiver<Item>,
}

impl<Item> futures_util::Stream for Items<'_, Item> {
    type Item = Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().item_rx.poll_recv(cx)
    }
}

/// Defines the item and final types of a streamed reply.
pub trait StreamReplyMessage: Message<Reply = StreamReply<Self::Item, Self::Final>> {
    /// One streamed item.
    type Item: Send + 'static;

    /// The final value.
    type Final: Send + 'static;
}

/// Marks a message handled by [`StreamHandler`].
pub trait StreamMessage: StreamReplyMessage + Message<Kind = StreamKind> {}

pin_project! {
    pub(crate) struct CompleteReply<A: Actor, F, R> {
        // Cancellation reports before the user future is destroyed.
        reply: Option<DispatchReply<'static, A, R>>,
        #[pin]
        future: F,
    }
}

impl<A: Actor, F, R> CompleteReply<A, F, R> {
    pub(crate) fn new(future: F, reply: DispatchReply<'static, A, R>) -> Self {
        Self {
            reply: Some(reply),
            future,
        }
    }
}

impl<A, F, R> Future for CompleteReply<A, F, R>
where
    A: Actor,
    F: Future<Output = R>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let value = std::task::ready!(this.future.poll(task));
        this.reply
            .take()
            .expect("reply completion runs exactly once")
            .complete(value);
        Poll::Ready(())
    }
}

pin_project! {
    struct FinishStream<F, T> {
        #[pin]
        future: F,
        final_tx: Option<oneshot::Sender<T>>,
    }
}

impl<F, T> FinishStream<F, T> {
    fn new(future: F, final_tx: oneshot::Sender<T>) -> Self {
        Self {
            future,
            final_tx: Some(final_tx),
        }
    }
}

impl<F, T> Future for FinishStream<F, T>
where
    F: Future<Output = T>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let value = std::task::ready!(this.future.poll(task));
        if let Some(final_tx) = this.final_tx.take() {
            let _ = final_tx.send(value);
        }
        Poll::Ready(())
    }
}
