use super::*;

struct SelfCaller;

#[actor(mailbox = 8, interleaved = dynamic)]
impl Actor for SelfCaller {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct Echo(u8);

impl Handler<Echo> for SelfCaller {
    async fn handle(message: Echo, _cx: Cx<'_, Self>) -> u8 {
        message.0
    }
}

#[derive(Message)]
#[message(reply = u8)]
struct SelfCall {
    value: u8,
    entered: Option<oneshot::Sender<()>>,
}

impl Handler<SelfCall> for SelfCaller {
    async fn handle(message: SelfCall, cx: Cx<'_, Self>) -> u8 {
        if let Some(entered) = message.entered {
            let _ = entered.send(());
        }
        cx.try_call(Echo(message.value)).unwrap().await.unwrap()
    }
}

#[derive(Message)]
#[message(reply = ())]
struct ExclusiveSelfCall {
    observed: oneshot::Sender<Response<u8>>,
    polled: oneshot::Sender<()>,
}

impl Handler<ExclusiveSelfCall> for SelfCaller {
    async fn handle(message: ExclusiveSelfCall, mut cx: Cx<'_, Self>) {
        let awaited = cx.try_call(Echo(1)).unwrap();
        let observed = cx.try_call(Echo(2)).unwrap();
        let _ = message.observed.send(observed);
        let _guard = cx.exclusive();
        let _ = message.polled.send(());
        awaited.await.unwrap();
    }
}

#[tokio::test]
async fn nonexclusive_self_calls_progress_but_exclusive_self_call_waits() {
    let options =
        SpawnOptions::<SelfCaller>::default().with_max_in_flight(NonZeroUsize::new(4).unwrap());
    let mut owner = spawn_with::<SelfCaller>((), options);
    let actor = owner.actor_ref();

    assert_eq!(
        watchdog(actor.call(SelfCall {
            value: 3,
            entered: None,
        }))
        .await,
        Ok(3)
    );
    assert_eq!(
        watchdog(actor.call(SelfCall {
            value: 4,
            entered: None,
        }))
        .await,
        Ok(4)
    );

    let (observed_tx, observed_rx) = oneshot::channel();
    let (polled_tx, polled_rx) = oneshot::channel();
    let outer = actor
        .try_call(ExclusiveSelfCall {
            observed: observed_tx,
            polled: polled_tx,
        })
        .unwrap();
    let observed = watchdog(observed_rx).await.unwrap();
    watchdog(polled_rx).await.unwrap();

    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        loac::ShutdownStatus::Requested
    );
    assert_eq!(
        watchdog(outer).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(
        watchdog(observed).await,
        Err(CallError::BeforeDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
}

#[tokio::test]
async fn self_call_waits_when_it_owns_the_only_reply_slot() {
    let options =
        SpawnOptions::<SelfCaller>::default().with_max_in_flight(NonZeroUsize::new(1).unwrap());
    let mut owner = spawn_with::<SelfCaller>((), options);
    let actor = owner.actor_ref();
    let (entered_tx, entered_rx) = oneshot::channel();
    let response = actor
        .try_call(SelfCall {
            value: 1,
            entered: Some(entered_tx),
        })
        .unwrap();

    watchdog(entered_rx).await.unwrap();
    assert_eq!(
        owner.request_shutdown(Shutdown::Kill),
        loac::ShutdownStatus::Requested
    );
    assert_eq!(
        watchdog(response).await,
        Err(CallError::DuringDispatch(ExitReason::Killed))
    );
    assert_eq!(watchdog(owner.wait()).await.reason(), ExitReason::Killed);
}
