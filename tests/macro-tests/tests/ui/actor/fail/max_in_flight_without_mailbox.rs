// Handler dispatch requires a mailbox.
struct MissingMailbox;

#[actor_api::actor(max_in_flight = 4)]
impl actor_api::Actor for MissingMailbox {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
