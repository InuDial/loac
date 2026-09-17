//! Uses scoped scheduler leases inside cx futures.
//!
//! An exclusive guard pauses other actor work until its destructor runs.
//!
//! ```console
//! cargo run -p loac --example cx_exclusive
//! ```

use std::time::Duration;

use loac::prelude::*;
use tokio::sync::oneshot;

struct Counter(u64);

#[actor(mailbox, interleaved)]
impl Actor for Counter {
    type SpawnArgs = u64;

    async fn init(value: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self(value)
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Read;

impl DispatchHandler<Read> for Counter {
    fn handle(
        &mut self,
        _message: Read,
        _scope: &mut ActorScope<Self>,
    ) -> impl loac::IntoReply<Self, Read> {
        self.0.ready()
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct AddExclusive {
    amount: u64,
    started: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl Handler<AddExclusive> for Counter {
    async fn handle(message: AddExclusive, mut cx: Cx<'_, Self>) -> u64 {
        let mut guard = cx.exclusive();
        let _ = message.started.send(());
        let _ = message.resume.await;
        guard.with(|actor, _| {
            actor.0 += message.amount;
            actor.0
        })
    }
}

#[derive(Message)]
#[message(stream = u8, reply = u8)]
struct StreamExclusive;

impl StreamHandler<StreamExclusive> for Counter {
    async fn handle<'a, W>(
        _message: StreamExclusive,
        mut out: StreamOut<'a, W>,
        mut cx: Cx<'a, Self>,
    ) -> u8
    where
        W: Writer<u8> + Send + 'a,
    {
        let mut guard = cx.exclusive();
        let next = guard.with(|actor, _| {
            actor.0 += 1;
            actor.0 as u8
        });
        let _ = out.write(next).await;
        next
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = loac::spawn::<Counter>(0);
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();

    let addition = owner.try_call(AddExclusive {
        amount: 5,
        started: started_tx,
        resume: resume_rx,
    })?;
    started_rx.await?;

    let mut read = owner.try_call(Read)?;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err(),
        "exclusive cx work pauses mailbox dispatch"
    );

    resume_tx
        .send(())
        .expect("the exclusive cx future retains the resume receiver");
    assert_eq!(addition.await?, 5);
    assert_eq!(read.await?, 5);

    let mut stream = owner.call(StreamExclusive).await?;
    assert_eq!(stream.recv().await, Some(6));
    assert_eq!(stream.recv().await, None);
    assert_eq!(stream.finish().await?, 6);

    assert_eq!(owner.call(Read).await?, 6);
    let status = owner.shutdown(loac::Shutdown::Drain).await;
    assert_eq!(status.reason(), loac::ExitReason::Drained);
    Ok(())
}
