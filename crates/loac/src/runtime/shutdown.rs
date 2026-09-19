use super::*;

pub(crate) async fn stop_actor<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    control: &Control,
    scheduler: &mut ActorScheduler<A>,
) -> ExitStatus {
    match close_and_discard(inbox, control, Mode::Stopping).await {
        DiscardOutcome::Complete => {}
        DiscardOutcome::ModeChanged => match control.mode() {
            Mode::Killing => return kill_actor(access, inbox, control, scheduler).await,
            Mode::Failing => return fail_actor(access, inbox, control, scheduler).await,
            // Lifecycle cannot return to a graceful mode. Aborting and Exited
            // belong to ActorTask's outer drop/publication path, which cannot
            // repoll this inner future after committing either state.
            mode => unreachable!("stop discard observed impossible mode: {mode:?}"),
        },
    }
    match finish_replies::<A>(control, scheduler).await {
        Work::Complete(()) => {}
        Work::Killed => return kill_actor(access, inbox, control, scheduler).await,
        Work::Panicked | Work::DropPanicked(()) => {
            return fail_actor(access, inbox, control, scheduler).await;
        }
    }

    match graceful_finish(access, control, Shutdown::Stop, ExitReason::Stopped).await {
        Work::Complete(()) => access
            .state()
            .children()
            .terminal_status(ExitReason::Stopped),
        Work::Killed => kill_actor(access, inbox, control, scheduler).await,
        Work::Panicked | Work::DropPanicked(()) => {
            fail_actor(access, inbox, control, scheduler).await
        }
    }
}

pub(crate) async fn drain_actor<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    inner: &Arc<ActorInner<A>>,
    scheduler: &mut ActorScheduler<A>,
) -> ExitStatus {
    let control = &inner.control;
    // Admission physically enqueues under the lifecycle transaction, so this
    // queue is stable once Drain commits. Capacity permits that never reached
    // admission are not accepted work and must not extend graceful shutdown.
    inbox.close();
    let mut inbox_drained = inbox.is_empty();
    loop {
        match control.mode() {
            Mode::Killing => {
                return kill_actor(access, inbox, control, scheduler).await;
            }
            Mode::Failing => {
                return fail_actor(access, inbox, control, scheduler).await;
            }
            Mode::Running | Mode::Draining | Mode::Stopping => {}
            Mode::Exited(status) => return status,
            Mode::Aborting => {
                return ExitStatus::new(ExitReason::Aborted, SubtreeStatus::Unconfirmed);
            }
        }

        if !inbox_drained && inbox.is_empty() {
            inbox_drained = true;
        }

        let turn = AssertUnwindSafe(drain_turn(access, inbox, inner, scheduler, !inbox_drained))
            .catch_unwind()
            .await;

        let turn = match turn {
            Ok(turn) => turn,
            Err(payload) => {
                control.contain_panic(payload);
                return fail_actor(access, inbox, control, scheduler).await;
            }
        };

        match turn {
            DrainTurn::RepliesFinished => break,
            DrainTurn::Scheduled(SchedulerTurn::LifecycleHint | SchedulerTurn::Progress) => {}
            DrainTurn::Scheduled(SchedulerTurn::Child(event)) => {
                match handle_child_exit(access, event, control).await {
                    Work::Complete(()) => {}
                    Work::Killed => {
                        return kill_actor(access, inbox, control, scheduler).await;
                    }
                    Work::Panicked | Work::DropPanicked(()) => {
                        control.begin_failure();
                        return fail_actor(access, inbox, control, scheduler).await;
                    }
                }
            }
            DrainTurn::Scheduled(SchedulerTurn::InboxClosed) => {
                inbox_drained = true;
            }
        }
    }

    match graceful_finish(access, control, Shutdown::Drain, ExitReason::Drained).await {
        Work::Complete(()) => access
            .state()
            .children()
            .terminal_status(ExitReason::Drained),
        Work::Killed => kill_actor(access, inbox, control, scheduler).await,
        Work::Panicked | Work::DropPanicked(()) => {
            fail_actor(access, inbox, control, scheduler).await
        }
    }
}

async fn finish_replies<A: Actor>(control: &Control, scheduler: &mut ActorScheduler<A>) -> Work {
    while !RuntimeScheduler::is_idle(scheduler) {
        match control.mode() {
            Mode::Killing => return Work::Killed,
            Mode::Failing => return Work::Panicked,
            Mode::Running | Mode::Draining | Mode::Stopping => {}
            Mode::Aborting | Mode::Exited(_) => return Work::Killed,
        }

        let result = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                () = control.actor_notified() => {}
                () = std::future::poll_fn(|task| {
                    RuntimeScheduler::poll_actor_replies(
                        scheduler,
                        control,
                        Mode::Stopping,
                        task,
                    )
                }) => {}
            }
        })
        .catch_unwind()
        .await;

        if let Err(payload) = result {
            control.contain_panic(payload);
            return Work::Panicked;
        }
    }

    Work::Complete(())
}

/// Graceful shutdown is post-order: a parent finishes the work retained by the
/// selected mode, then waits for children, and only then runs its cleanup hook.
pub(crate) async fn graceful_finish<A: Actor>(
    access: &mut ActorAccess<A>,
    control: &Control,
    shutdown: Shutdown,
    reason: ExitReason,
) -> Work {
    // Keep child submission inside the biased lifecycle guard. A Kill already
    // committed before this poll must win before Stop or Drain reaches children.
    match await_actor_work(
        async {
            access.state().children().request_all(shutdown);
            access.state().children().wait_all().await;
        },
        control,
    )
    .await
    {
        Work::Complete(()) => {}
        Work::Killed => return Work::Killed,
        Work::Panicked => return Work::Panicked,
        Work::DropPanicked(()) => return Work::DropPanicked(()),
    }

    await_actor_work(
        async {
            let (actor, mut scope) = access.stop_parts();
            actor.on_stop(reason, &mut scope).await;
        },
        control,
    )
    .await
}

pub(crate) async fn kill_actor<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    control: &Control,
    scheduler: &mut ActorScheduler<A>,
) -> ExitStatus {
    // Commit subtree cancellation before running arbitrary Drop code from actor
    // work. Children can then begin terminating even if a destructor is slow.
    inbox.close();
    access.state().children().request_all(Shutdown::Kill);
    RuntimeScheduler::clear(scheduler, control);
    let mut expected_mode = Mode::Killing;
    loop {
        match close_and_discard(inbox, control, expected_mode).await {
            DiscardOutcome::Complete => break,
            DiscardOutcome::ModeChanged => expected_mode = control.mode(),
        }
    }
    access.state().children().wait_all().await;
    access
        .state()
        .children()
        .terminal_status(ExitReason::Killed)
}

pub(crate) async fn fail_actor<A: Actor>(
    access: &mut ActorAccess<A>,
    inbox: &mut ActorInbox<A>,
    control: &Control,
    scheduler: &mut ActorScheduler<A>,
) -> ExitStatus {
    control.begin_failure();
    let reason = match control.mode() {
        Mode::Killing => ExitReason::Killed,
        Mode::Aborting => ExitReason::Aborted,
        Mode::Exited(status) => return status,
        Mode::Running | Mode::Draining | Mode::Stopping | Mode::Failing => ExitReason::Panicked,
    };
    inbox.close();
    access.state().children().request_all(Shutdown::Kill);
    RuntimeScheduler::clear(scheduler, control);
    let mut expected_mode = control.mode();
    loop {
        match close_and_discard(inbox, control, expected_mode).await {
            DiscardOutcome::Complete => break,
            DiscardOutcome::ModeChanged => expected_mode = control.mode(),
        }
    }
    access.state().children().wait_all().await;
    access.state().children().terminal_status(reason)
}

// Handler dispatch never precedes successful initialization.
// Both paths therefore own no scheduled user values.
pub(crate) async fn kill_uninitialized<A: Actor>(
    state: &mut ScopeState<A>,
    inbox: &mut ActorInbox<A>,
    control: &Control,
) -> ExitStatus {
    inbox.close();
    state.children().request_all(Shutdown::Kill);
    close_discarded(inbox, control, Mode::Killing).await;
    state.children().wait_all().await;
    state.children().terminal_status(ExitReason::Killed)
}

pub(crate) async fn fail_uninitialized<A: Actor>(
    state: &mut ScopeState<A>,
    inbox: &mut ActorInbox<A>,
    control: &Control,
) -> ExitStatus {
    control.begin_failure();
    let reason = match control.mode() {
        Mode::Killing => ExitReason::Killed,
        Mode::Aborting => ExitReason::Aborted,
        Mode::Exited(status) => return status,
        Mode::Running | Mode::Draining | Mode::Stopping | Mode::Failing => ExitReason::Panicked,
    };
    inbox.close();
    state.children().request_all(Shutdown::Kill);
    close_discarded(inbox, control, control.mode()).await;
    state.children().wait_all().await;
    state.children().terminal_status(reason)
}

async fn close_discarded<A: Actor>(
    inbox: &mut ActorInbox<A>,
    control: &Control,
    mut expected_mode: Mode,
) {
    loop {
        match close_and_discard(inbox, control, expected_mode).await {
            DiscardOutcome::Complete => return,
            DiscardOutcome::ModeChanged => expected_mode = control.mode(),
        }
    }
}

pub(crate) const TEARDOWN_DROP_BUDGET: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiscardOutcome {
    Complete,
    ModeChanged,
}

/// Closes the inbox and drops its accepted envelopes in bounded batches.
///
/// Each envelope may run arbitrary user destructors. The lifecycle mode is
/// checked before selecting a value and again after dropping it, so a transition
/// returns [`DiscardOutcome::ModeChanged`] before another value is selected.
/// Every uninterrupted mode pass yields after [`TEARDOWN_DROP_BUDGET`] drops so
/// a large accepted queue cannot monopolize a current-thread executor.
pub(crate) async fn close_and_discard<A: Actor>(
    inbox: &mut ActorInbox<A>,
    control: &Control,
    expected_mode: Mode,
) -> DiscardOutcome {
    inbox.close();
    let mut dropped = 0;
    loop {
        if control.mode() != expected_mode {
            return DiscardOutcome::ModeChanged;
        }

        if !inbox.try_discard() {
            return DiscardOutcome::Complete;
        }

        if control.mode() != expected_mode {
            return DiscardOutcome::ModeChanged;
        }
        dropped += 1;
        if dropped == TEARDOWN_DROP_BUDGET {
            dropped = 0;
            tokio::task::yield_now().await;
        }
    }
}
