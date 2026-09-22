use super::*;

#[tokio::test]
async fn unadmitted_mailbox_permit_does_not_extend_drain() {
    let (inner, inbox) = test_actor_inner(1);
    let permit = inner
        .sender
        .reserve_owned()
        .await
        .expect("the test mailbox is open");
    let control = &inner.control;
    let scope = scope_state(&inner);
    assert_eq!(control.request(Shutdown::Drain), ShutdownStatus::Requested);

    let options = <TestActor as ActorConfig>::Options::default()
        .with_max_in_flight(NonZeroUsize::new(1).unwrap());
    let (_, _, scheduler) = TestActor::open(&options);
    let actor_ref = actor_ref(&inner);
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        run_actor::<TestActor>((), actor_ref, scope, inbox, scheduler),
    )
    .await
    .expect("an unadmitted capacity permit must not hold Drain open");

    assert_eq!(
        status,
        ExitStatus::new(ExitReason::Drained, SubtreeStatus::Terminated)
    );
    drop(permit);
}

#[tokio::test]
async fn committed_kill_prevents_a_graceful_child_request() {
    // The child mode distinguishes biased Kill observation from an incorrect
    // first poll of the guarded future: request_all would synchronously commit
    // Stop before graceful_finish could return Work::Killed.
    let (child, _child_inbox) = test_actor_inner(1);

    let (inner, _inbox) = test_actor_inner(1);
    let control = &inner.control;
    let mut scope = scope_state(&inner);
    let child_ref = ActorRef::new(Arc::clone(&child));
    scope.children.insert_ref(&child_ref);
    let actor_ref = actor_ref(&inner);
    let mut access = ActorAccess::new(actor_ref, TestActor, scope);

    assert_eq!(control.request(Shutdown::Kill), ShutdownStatus::Requested);
    assert!(matches!(
        graceful_finish(&mut access, control, Shutdown::Stop, ExitReason::Stopped,).await,
        Work::Killed
    ));
    assert_eq!(child.control.mode(), Mode::Running);
}

// A truncated reply sweep must yield before polling another ready lane.
// Otherwise ready mailbox traffic can erase the cooperative boundary.
#[test]
fn truncated_reply_sweep_yields_before_ready_mailbox() {
    // Seventeen ready replies equal the poll budget plus one.
    const REPLIES: usize = 17;

    let mailbox_dispatches = Arc::new(AtomicUsize::new(0));
    let replies_polled = Arc::new(AtomicUsize::new(0));
    let (inner, mut inbox) = test_actor_inner(1);
    enqueue_test_envelope(&inner, CountEnvelope(Arc::clone(&mailbox_dispatches)));

    let scope = scope_state(&inner);
    let options = <TestActor as ActorConfig>::Options::default()
        .with_max_in_flight(NonZeroUsize::new(REPLIES).unwrap());
    let (_, _, mut scheduler) = TestActor::open(&options);
    for _ in 0..REPLIES {
        let replies_polled = Arc::clone(&replies_polled);
        scheduler.state().push_test(async move {
            replies_polled.fetch_add(1, Ordering::SeqCst);
        });
    }
    let actor_ref = actor_ref(&inner);
    let mut access = ActorAccess::new(actor_ref, TestActor, scope);
    // Start at replies. The old path continued to the ready mailbox.
    scheduler.state().cursor = ReplyLane::Reply;
    let mut task = Context::from_waker(Waker::noop());

    {
        let mut turn = std::pin::pin!(actor_turn(
            &mut access,
            &mut inbox,
            &inner,
            &mut scheduler,
            true,
            Mode::Running,
        ));
        assert!(turn.as_mut().poll(&mut task).is_pending());
    }

    let replies_polled = replies_polled.load(Ordering::SeqCst);
    assert!(0 < replies_polled && replies_polled < REPLIES);
    assert_eq!(mailbox_dispatches.load(Ordering::SeqCst), 0);
    assert!(scheduler.state().has_replies());
    assert_eq!(scheduler.state().cursor, ReplyLane::ChildExit);
}

#[tokio::test]
async fn child_kill_commits_before_actor_work_is_dropped() {
    // Active replies and queued envelopes may both run arbitrary destructors.
    // Observing the child mode from each Drop rejects any teardown that merely
    // waits for children after clearing local work instead of cancelling first.
    let child_owner = loac::spawn::<TestActor>(());
    let child_ref = child_owner.actor_ref();
    let child_inner = Arc::clone(&child_ref.0);

    let active_observed_kill = Arc::new(AtomicBool::new(false));
    let queued_observed_kill = Arc::new(AtomicBool::new(false));
    let (inner, mut inbox) = test_actor_inner(1);
    enqueue_test_envelope(
        &inner,
        ChildKillDropProbe {
            child: Arc::clone(&child_inner),
            observed_kill: Arc::clone(&queued_observed_kill),
        },
    );
    assert_eq!(
        inner.control.request(Shutdown::Kill),
        ShutdownStatus::Requested
    );

    let mut scope = scope_state(&inner);
    scope.children.insert_ref(&child_ref);
    let options =
        <TestActor as ActorConfig>::Options::default().with_max_in_flight(NonZeroUsize::MIN);
    let (_, _, mut scheduler) = TestActor::open(&options);
    let actor_ref = actor_ref(&inner);
    let mut access = ActorAccess::new(actor_ref, TestActor, scope);
    scheduler.state().push_test(ChildKillDropProbe {
        child: child_inner,
        observed_kill: Arc::clone(&active_observed_kill),
    });

    assert_eq!(
        kill_actor(&mut access, &mut inbox, &inner.control, &mut scheduler,).await,
        ExitStatus::new(ExitReason::Killed, SubtreeStatus::Terminated)
    );
    assert!(active_observed_kill.load(Ordering::SeqCst));
    assert!(queued_observed_kill.load(Ordering::SeqCst));
    drop(child_owner);
}
