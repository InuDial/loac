struct MissingChildren;

#[actor_api::actor(max_children = 8)]
impl actor_api::Actor for MissingChildren {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
