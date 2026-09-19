use super::shutdown::{
    drain_actor, fail_actor, fail_uninitialized, kill_actor, kill_uninitialized, stop_actor,
};
use super::*;

// Fields drop in declaration order.
// Scheduled futures may target the pinned actor cell.
// Therefore, `scheduler` must drop before `access`.
struct RunningActor<A: Actor> {
    scheduler: ActorScheduler<A>,
    access: ActorAccess<A>,
}

pub(crate) async fn run_actor<A: Actor>(
    args: A::SpawnArgs,
    actor_ref: ActorRef<A>,
    mut state: ScopeState<A>,
    mut inbox: ActorInbox<A>,
    scheduler: ActorScheduler<A>,
) -> ExitStatus {
    let inner = Arc::clone(&actor_ref.0);
    let control = &inner.control;
    let initialized = if let Some(_permit) = control.begin_initialization() {
        let mut scope = state.actor_scope(&actor_ref);
        match panic::catch_unwind(AssertUnwindSafe(|| A::init(args, &mut scope))) {
            Ok(init) => await_actor_work(init, control).await,
            Err(payload) => {
                control.contain_panic(payload);
                Work::Panicked
            }
        }
    } else {
        control.drop_user_value(args);
        Work::Killed
    };

    let actor = match initialized {
        Work::Complete(actor) => actor,
        Work::Killed => {
            return kill_uninitialized(&mut state, &mut inbox, control).await;
        }
        Work::Panicked => {
            return fail_uninitialized(&mut state, &mut inbox, control).await;
        }
        Work::DropPanicked(actor) => {
            // The init frame failed after producing actor state.
            // Descendant cancellation must precede arbitrary actor Drop code.
            state.children().request_all(Shutdown::Kill);
            control.drop_user_value(actor);
            return fail_uninitialized(&mut state, &mut inbox, control).await;
        }
    };

    let mut running = RunningActor {
        scheduler,
        access: ActorAccess::new(actor_ref, actor, state),
    };

    loop {
        match control.mode() {
            Mode::Running => {}
            Mode::Draining => {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    running
                        .access
                        .with_actor(|actor| actor.on_shutdown(Shutdown::Drain))
                })) {
                    Ok(()) => {}
                    Err(payload) => {
                        control.contain_panic(payload);
                        return fail_actor(
                            &mut running.access,
                            &mut inbox,
                            control,
                            &mut running.scheduler,
                        )
                        .await;
                    }
                }
                return drain_actor(
                    &mut running.access,
                    &mut inbox,
                    &inner,
                    &mut running.scheduler,
                )
                .await;
            }
            Mode::Stopping => {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    running
                        .access
                        .with_actor(|actor| actor.on_shutdown(Shutdown::Stop))
                })) {
                    Ok(()) => {}
                    Err(payload) => {
                        control.contain_panic(payload);
                        return fail_actor(
                            &mut running.access,
                            &mut inbox,
                            control,
                            &mut running.scheduler,
                        )
                        .await;
                    }
                }
                return stop_actor(
                    &mut running.access,
                    &mut inbox,
                    control,
                    &mut running.scheduler,
                )
                .await;
            }
            Mode::Killing => {
                return kill_actor(
                    &mut running.access,
                    &mut inbox,
                    control,
                    &mut running.scheduler,
                )
                .await;
            }
            Mode::Failing => {
                return fail_actor(
                    &mut running.access,
                    &mut inbox,
                    control,
                    &mut running.scheduler,
                )
                .await;
            }
            Mode::Exited(status) => return status,
            Mode::Aborting => {
                return ExitStatus::new(ExitReason::Aborted, SubtreeStatus::Unconfirmed);
            }
        }

        let turn = AssertUnwindSafe(actor_turn(
            &mut running.access,
            &mut inbox,
            &inner,
            &mut running.scheduler,
            true,
            Mode::Running,
        ))
        .catch_unwind()
        .await;

        let turn = match turn {
            Ok(turn) => turn,
            Err(payload) => {
                control.contain_panic(payload);
                return fail_actor(
                    &mut running.access,
                    &mut inbox,
                    control,
                    &mut running.scheduler,
                )
                .await;
            }
        };

        match turn {
            SchedulerTurn::LifecycleHint | SchedulerTurn::Progress => {}
            SchedulerTurn::Child(event) => {
                match handle_child_exit(&mut running.access, event, control).await {
                    Work::Complete(()) => {}
                    Work::Killed => {
                        return kill_actor(
                            &mut running.access,
                            &mut inbox,
                            control,
                            &mut running.scheduler,
                        )
                        .await;
                    }
                    Work::Panicked | Work::DropPanicked(()) => {
                        control.begin_failure();
                        return fail_actor(
                            &mut running.access,
                            &mut inbox,
                            control,
                            &mut running.scheduler,
                        )
                        .await;
                    }
                }
            }
            SchedulerTurn::InboxClosed => {
                control.begin_failure();
                return fail_actor(
                    &mut running.access,
                    &mut inbox,
                    control,
                    &mut running.scheduler,
                )
                .await;
            }
        }
    }
}

pub(crate) enum DrainTurn {
    Scheduled(SchedulerTurn),
    RepliesFinished,
}

// The scheduler owns every dispatched reply.
pub(crate) async fn drain_turn<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    inner: &Arc<ActorInner<A>>,
    scheduler: &mut ActorScheduler<A>,
    receive_messages: bool,
) -> DrainTurn {
    if !receive_messages && RuntimeScheduler::is_idle(scheduler) {
        return DrainTurn::RepliesFinished;
    }

    DrainTurn::Scheduled(
        actor_turn(
            access,
            inbox,
            inner,
            scheduler,
            receive_messages,
            Mode::Draining,
        )
        .await,
    )
}

pub(crate) async fn handle_child_exit<A: Actor>(
    access: &mut ActorAccess<A>,
    event: ChildExit,
    control: &Control,
) -> Work {
    if !access.state().children().reap(&event) {
        return Work::Complete(());
    }

    let Some(permit) = control.begin_child_hook() else {
        return Work::Complete(());
    };

    run_child_exit_hook(access, event, control, permit).await
}

/// Makes the private gate proof mandatory at the only user hook call site.
async fn run_child_exit_hook<A: Actor>(
    access: &mut ActorAccess<A>,
    event: ChildExit,
    control: &Control,
    _permit: HookEntryPermit,
) -> Work {
    await_actor_work(
        async {
            let (actor, mut scope) = access.parts();
            actor.on_child_exit(event, &mut scope).await;
        },
        control,
    )
    .await
}

// Scheduler profiles own their eligible lane rotation.
// Lifecycle keeps first poll rights across every profile.
pub(crate) async fn actor_turn<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    inner: &Arc<ActorInner<A>>,
    scheduler: &mut ActorScheduler<A>,
    receive_messages: bool,
    expected_mode: Mode,
) -> SchedulerTurn {
    let control = &inner.control;
    let fair_turn = std::future::poll_fn(|task| {
        let mut turn = TurnContext {
            access,
            inbox,
            inner,
            receive_messages,
            expected_mode,
        };
        RuntimeScheduler::poll_turn(scheduler, &mut turn, task)
    });

    // The biased select lets lifecycle notifications, especially Kill,
    // preempt the fair scheduler turn.
    tokio::select! {
        biased;
        () = control.actor_notified() => SchedulerTurn::LifecycleHint,
        turn = fair_turn => turn,
    }
}
