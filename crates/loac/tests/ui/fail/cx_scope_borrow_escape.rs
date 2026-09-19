use loac::prelude::*;

struct Worker;

#[actor(mailbox)]
impl Actor for Worker {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn escape<'a>(cx: &'a mut Cx<'_, Worker>) -> &'a mut ActorScope<'a, Worker> {
    cx.with(|_actor, scope| scope)
}

fn main() {}
