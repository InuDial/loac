struct Invalid;

#[actor_api::actor(children = 8)]
impl actor_api::Actor for Invalid {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
