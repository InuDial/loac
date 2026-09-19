struct MissingMailbox;

#[actor_api::actor(mailbox_capacity = 8)]
impl actor_api::Actor for MissingMailbox {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
