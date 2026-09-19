// Named dynamic defaults must fail before actor spawning.
const ZERO: usize = 0;

struct Zero;

#[actor_api::actor(mailbox, mailbox_capacity = dynamic(ZERO))]
impl actor_api::Actor for Zero {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut actor_api::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {}
