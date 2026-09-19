const ZERO: usize = 0;

struct Zero;

#[actor_api::actor(mailbox, max_in_flight = dynamic(ZERO))]
impl actor_api::Actor for Zero {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
