// Repeated options must not silently select one handler limit.
struct Duplicate;

#[actor_api::actor(mailbox, max_in_flight = 8, max_in_flight = unbounded)]
impl actor_api::Actor for Duplicate {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
