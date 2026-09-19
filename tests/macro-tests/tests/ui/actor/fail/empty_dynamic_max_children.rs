// Parentheses must provide a custom default.
struct EmptyDynamic;

#[actor_api::actor(children, max_children = dynamic())]
impl actor_api::Actor for EmptyDynamic {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
