//! Runs independent root actors with separate lifecycle owners.
//! Dropping one owner requests Kill without affecting its peer.

use loac::prelude::*;

struct Worker {
    factor: u64,
}

#[actor(mailbox)]
impl Actor for Worker {
    type SpawnArgs = u64;

    async fn init(factor: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self { factor }
    }
}

#[derive(Message)]
#[message(reply = u64)]
struct Multiply(u64);

impl Handler<Multiply> for Worker {
    async fn handle(message: Multiply, mut cx: Cx<'_, Self>) -> u64 {
        cx.with(|actor, _| actor.factor * message.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first_owner = loac::spawn::<Worker>(2);
    let second_owner = loac::spawn::<Worker>(3);
    let first = first_owner.actor_ref();

    let (first_result, second_result) =
        tokio::join!(first.call(Multiply(5)), second_owner.call(Multiply(5)));
    assert_eq!(first_result?, 10);
    assert_eq!(second_result?, 15);

    drop(first_owner);
    assert_eq!(first.closed().await.reason(), loac::ExitReason::Killed);
    assert_eq!(second_owner.call(Multiply(4)).await?, 12);
    assert_eq!(
        second_owner.shutdown(loac::Shutdown::Drain).await.reason(),
        loac::ExitReason::Drained
    );
    Ok(())
}
