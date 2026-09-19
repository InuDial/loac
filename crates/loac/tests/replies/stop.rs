use super::*;

struct StopActor;

#[actor(mailbox, mailbox_capacity = 4, max_in_flight = 3)]
impl Actor for StopActor {
    type SpawnArgs = ();

    async fn init(_args: Self::SpawnArgs, _scope: &mut ActorScope<'_, Self>) -> Self {
        Self
    }
}

#[derive(Message)]
#[message(reply = ())]
struct StopWork {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Handler<StopWork> for StopActor {
    async fn handle(message: StopWork, _cx: Cx<'_, Self>) {
        let _ = message.entered.send(());
        let _ = message.release.await;
    }
}

#[tokio::test]
async fn graceful_shutdown_waits_for_every_scheduled_reply() {
    // All three replies must start concurrently.
    // Each partial release must leave shutdown pending.
    // That proves shutdown waits for every reply.
    for (shutdown, expected) in [
        (Shutdown::Stop, ExitReason::Stopped),
        (Shutdown::Drain, ExitReason::Drained),
    ] {
        let mut owner = loac::spawn::<StopActor>(());
        let actor = owner.actor_ref();
        let mut entered = Vec::new();
        let mut releases = Vec::new();
        let mut replies = Vec::new();

        for _ in 0..3 {
            let (entered_tx, entered_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            replies.push(
                actor
                    .try_call(StopWork {
                        entered: entered_tx,
                        release: release_rx,
                    })
                    .unwrap(),
            );
            entered.push(entered_rx);
            releases.push(release_tx);
        }
        for entered in entered {
            watchdog(entered).await.unwrap();
        }

        assert_eq!(
            owner.request_shutdown(shutdown),
            loac::ShutdownStatus::Requested
        );
        let mut stopped = Box::pin(owner.wait());
        assert!(poll_once(stopped.as_mut()).await.is_pending());

        let task_count = releases.len();
        for (index, (release, reply)) in releases.into_iter().zip(replies).enumerate() {
            release.send(()).unwrap();
            assert_eq!(watchdog(reply).await, Ok(()));
            if index + 1 < task_count {
                assert!(poll_once(stopped.as_mut()).await.is_pending());
            }
        }
        assert_eq!(watchdog(stopped).await.reason(), expected);
    }
}
