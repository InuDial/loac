// Repeated options must not silently select one child capacity.
struct Duplicate;

#[actor_api::actor(children, max_children = unbounded, max_children = 8)]
impl actor_api::Actor for Duplicate {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
