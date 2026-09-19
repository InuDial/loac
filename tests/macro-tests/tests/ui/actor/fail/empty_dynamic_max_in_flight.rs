// Parentheses must provide one max_in_flight default.
struct Empty;

#[actor_api::actor(mailbox, max_in_flight = dynamic())]
impl actor_api::Actor for Empty {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
