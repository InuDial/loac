struct MissingPolicy;

#[actor_api::actor(mailbox, max_in_flight)]
impl actor_api::Actor for MissingPolicy {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
