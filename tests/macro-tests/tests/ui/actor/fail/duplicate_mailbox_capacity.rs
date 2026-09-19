// Repeated options must not silently select one capacity.
struct Duplicate;

#[actor_api::actor(mailbox, mailbox_capacity = unbounded, mailbox_capacity = 8)]
impl actor_api::Actor for Duplicate {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
