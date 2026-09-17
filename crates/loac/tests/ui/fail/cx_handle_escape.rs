use loac::prelude::*;

struct Worker;

#[actor(mailbox, interleaved)]
impl Actor for Worker {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn escape(cx: Cx<'_, Worker>) {
    tokio::spawn(async move {
        let mut cx = cx;
        cx.with(|_actor, _scope| ());
    });
}

fn main() {}
