use loac::prelude::*;

struct Worker(usize);

#[actor(mailbox, interleaved)]
impl Actor for Worker {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self(0)
    }
}

fn escape<'a>(cx: &'a mut Cx<'a, Worker>) -> impl FnOnce() -> usize + 'a {
    cx.with(|actor, _scope| move || actor.0)
}

fn main() {}
