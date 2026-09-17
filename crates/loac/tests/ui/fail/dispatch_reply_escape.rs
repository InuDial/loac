use loac::prelude::*;

fn detach<'a, A, M>(
    actor: &'a mut A,
    message: M,
    scope: &'a mut ActorScope<'_, A>,
) -> impl IntoReply<A, M> + 'static
where
    A: DispatchHandler<M>,
    M: Message,
{
    actor.handle(message, scope)
}

fn main() {}
