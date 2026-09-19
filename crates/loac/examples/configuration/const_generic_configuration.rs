//! Reuses one const generic across actor options.
//! `mailbox, mailbox_capacity = dynamic(N)` makes `N` the spawn default.
//! `max_in_flight = N` fixes the limit for each actor type.

use loac::prelude::*;

struct Service<const N: usize>;

#[actor(mailbox, mailbox_capacity = dynamic(N), max_in_flight = N)]
impl<const N: usize> Actor for Service<N> {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = usize)]
struct ReadTypeParameter;

impl<const N: usize> Handler<ReadTypeParameter> for Service<N> {
    async fn handle(_message: ReadTypeParameter, _cx: Cx<'_, Self>) -> usize {
        N
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = loac::spawn::<Service<8>>(());
    assert_eq!(owner.call(ReadTypeParameter).await?, 8);

    assert_eq!(
        owner.shutdown(loac::Shutdown::Drain).await.reason(),
        loac::ExitReason::Drained
    );
    Ok(())
}
