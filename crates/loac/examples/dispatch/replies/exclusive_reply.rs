//! Pauses mailbox work until an exclusive actor-aware reply completes.

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
struct AddAfter {
    amount: u64,
    started: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl Handler<AddAfter> for Counter {
    async fn handle(message: AddAfter, mut cx: Cx<'_, Self>) -> u64 {
        let mut guard = cx.exclusive();
        message
            .started
            .send(())
            .expect("the example retains the started receiver");
        message
            .resume
            .await
            .expect("the example retains the resume sender");
        guard.with(|actor, _| {
            actor.0 += message.amount;
            actor.0
        })
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
    ) -> impl IntoReply<Self, Read> {
        self.0.ready()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = loac::spawn::<Counter>(10);
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();

    let addition = owner.try_call(AddAfter {
        amount: 5,
        started: started_tx,
        resume: resume_rx,
    })?;
    started_rx.await?;

    let mut read = owner.try_call(Read)?;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );

    resume_tx
        .send(())
        .expect("the leased reply retains the resume receiver");
    assert_eq!(addition.await?, 15);
    assert_eq!(read.await?, 15);

    assert_eq!(
        owner.shutdown(loac::Shutdown::Drain).await.reason(),
        loac::ExitReason::Drained
    );
    Ok(())
}
