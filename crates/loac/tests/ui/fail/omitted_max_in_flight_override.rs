use std::num::NonZeroUsize;

// A statically configured actor has no dynamic limit to override.
struct Defaulted;

#[loac::actor(mailbox)]
impl loac::Actor for Defaulted {
    type SpawnArgs = ();

    async fn init(_: (), _: &mut loac::ActorScope<'_, Self>) -> Self {
        Self
    }
}

fn main() {
    let _ = loac::SpawnOptions::<Defaulted>::default()
        .with_max_in_flight(NonZeroUsize::MIN);
}
