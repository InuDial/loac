#![allow(
    private_bounds,
    private_interfaces,
    reason = "public profile proofs remain hidden by the private runtime module"
)]

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    Actor, ChildExit,
    access::ReplySlot,
    mailbox::{ActorInbox, ActorInner, Control, Mode},
    runtime::ActorAccess,
    transport::{MessageConfig, MessageInbox, MessageSender, NoInbox, NoSender},
};

use super::{
    Disabled, ReplyLane, ReplyProfile, ScheduledFuture, SchedulerProfile, queue::ReplyPoll,
};

pub(crate) type ActorScheduler<A> = <A as MessageConfig>::Scheduler;

/// Result of one scheduler-owned actor turn.
pub(crate) enum SchedulerTurn {
    /// Notification or lifecycle mismatch.
    LifecycleHint,
    /// Mailbox dispatch or an actor-aware reply made progress.
    Progress,
    /// One direct child terminated.
    Child(ChildExit),
    /// The mailbox receiver closed.
    InboxClosed,
}

/// Borrows actor-task resources for one scheduler poll.
pub(crate) struct TurnContext<'a, A: Actor> {
    pub(crate) access: &'a mut ActorAccess<A>,
    pub(crate) inbox: &'a mut ActorInbox<A>,
    pub(crate) inner: &'a Arc<ActorInner<A>>,
    pub(crate) receive_messages: bool,
    pub(crate) expected_mode: Mode,
}

pub(crate) trait RuntimeScheduler<A: Actor>: Send + 'static {
    fn schedule<F>(&mut self, build: F)
    where
        F: FnOnce(ReplySlot) -> ScheduledFuture;

    fn is_idle(&mut self) -> bool;

    fn poll_actor_replies(
        &mut self,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> Poll<()>;

    fn poll_turn(
        &mut self,
        turn: &mut TurnContext<'_, A>,
        task: &mut Context<'_>,
    ) -> Poll<SchedulerTurn>;

    fn clear(&mut self, control: &Control);
}

pub trait ProfileRuntime<A: Actor, P: Send + 'static>: Send + 'static {
    fn schedule<F>(scheduler: &mut P, build: F)
    where
        F: FnOnce(ReplySlot) -> ScheduledFuture;

    fn is_idle(scheduler: &mut P) -> bool;

    fn poll_actor_replies(
        scheduler: &mut P,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> Poll<()>;

    fn poll_turn(
        scheduler: &mut P,
        turn: &mut TurnContext<'_, A>,
        task: &mut Context<'_>,
    ) -> Poll<SchedulerTurn>;

    fn clear(scheduler: &mut P, control: &Control);
}

pub struct DisabledRuntime;
pub struct MailboxRuntime;

// One associated runtime selects a monomorphized actor loop.
impl<A, P> RuntimeScheduler<A> for P
where
    A: Actor + MessageConfig<Scheduler = P>,
    P: SchedulerProfile<A>,
{
    fn schedule<F>(&mut self, build: F)
    where
        F: FnOnce(ReplySlot) -> ScheduledFuture,
    {
        P::Runtime::schedule(self, build);
    }

    fn is_idle(&mut self) -> bool {
        P::Runtime::is_idle(self)
    }

    fn poll_actor_replies(
        &mut self,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> Poll<()> {
        P::Runtime::poll_actor_replies(self, control, expected_mode, task)
    }

    fn poll_turn(
        &mut self,
        turn: &mut TurnContext<'_, A>,
        task: &mut Context<'_>,
    ) -> Poll<SchedulerTurn> {
        P::Runtime::poll_turn(self, turn, task)
    }

    fn clear(&mut self, control: &Control) {
        P::Runtime::clear(self, control);
    }
}

// Disabled has no mailbox or reply lanes.
// Child exits still need a supervision lane.
impl<A> ProfileRuntime<A, Disabled> for DisabledRuntime
where
    A: Actor + MessageConfig<Sender = NoSender, Inbox = NoInbox, Scheduler = Disabled>,
{
    fn schedule<F>(_scheduler: &mut Disabled, _build: F)
    where
        F: FnOnce(ReplySlot) -> ScheduledFuture,
    {
        unreachable!("an actor without a mailbox cannot schedule replies")
    }

    fn is_idle(_scheduler: &mut Disabled) -> bool {
        true
    }

    fn poll_actor_replies(
        _scheduler: &mut Disabled,
        _control: &Control,
        _expected_mode: Mode,
        _task: &mut Context<'_>,
    ) -> Poll<()> {
        Poll::Pending
    }

    fn poll_turn(
        _scheduler: &mut Disabled,
        turn: &mut TurnContext<'_, A>,
        task: &mut Context<'_>,
    ) -> Poll<SchedulerTurn> {
        if turn.inner.control.mode() != turn.expected_mode {
            return Poll::Ready(SchedulerTurn::LifecycleHint);
        }

        match poll_child(turn.access, task) {
            Some(turn) => Poll::Ready(turn),
            None => Poll::Pending,
        }
    }

    fn clear(_scheduler: &mut Disabled, _control: &Control) {}
}

impl<A, P> ProfileRuntime<A, P> for MailboxRuntime
where
    A: Actor + MessageConfig<Scheduler = P>,
    A::Sender: MessageSender<A>,
    A::Inbox: MessageInbox<A>,
    P: ReplyProfile<A>,
{
    fn schedule<F>(scheduler: &mut P, build: F)
    where
        F: FnOnce(ReplySlot) -> ScheduledFuture,
    {
        scheduler.state().schedule(build);
    }

    fn is_idle(scheduler: &mut P) -> bool {
        !scheduler.state().has_replies()
    }

    fn poll_actor_replies(
        scheduler: &mut P,
        control: &Control,
        expected_mode: Mode,
        task: &mut Context<'_>,
    ) -> Poll<()> {
        match scheduler.state().queue.poll(control, expected_mode, task) {
            ReplyPoll::Pending | ReplyPoll::Leased | ReplyPoll::BudgetExhausted => Poll::Pending,
            ReplyPoll::Progress => Poll::Ready(()),
        }
    }

    fn poll_turn(
        scheduler: &mut P,
        turn: &mut TurnContext<'_, A>,
        task: &mut Context<'_>,
    ) -> Poll<SchedulerTurn> {
        if turn.inner.control.mode() != turn.expected_mode {
            return Poll::Ready(SchedulerTurn::LifecycleHint);
        }

        if scheduler.state().queue.is_leased() {
            return match scheduler
                .state()
                .queue
                .poll(&turn.inner.control, turn.expected_mode, task)
            {
                ReplyPoll::Progress => Poll::Ready(SchedulerTurn::Progress),
                ReplyPoll::Pending | ReplyPoll::Leased | ReplyPoll::BudgetExhausted => {
                    Poll::Pending
                }
            };
        }

        let start = scheduler.state().cursor;
        let mut lane = start;
        loop {
            if turn.inner.control.mode() != turn.expected_mode {
                return Poll::Ready(SchedulerTurn::LifecycleHint);
            }
            let selected = match lane {
                ReplyLane::Mailbox
                    if turn.receive_messages && scheduler.state().has_dispatch_capacity() =>
                {
                    let mut dispatched = 0;
                    loop {
                        match turn.inbox.poll_recv(task) {
                            Poll::Ready(Some(envelope)) => {
                                envelope.dispatch(turn.access, scheduler, turn.inner);
                                dispatched += 1;
                            }
                            Poll::Ready(None) => break Some(SchedulerTurn::InboxClosed),
                            Poll::Pending
                                if dispatched > 0
                                    && start == ReplyLane::Reply
                                    && scheduler.state().has_replies() =>
                            {
                                break Some(SchedulerTurn::Progress);
                            }
                            Poll::Pending => break None,
                        }

                        if turn.inner.control.mode() != turn.expected_mode {
                            scheduler.state().cursor = lane.next();
                            return Poll::Ready(SchedulerTurn::LifecycleHint);
                        }
                        if dispatched == A::MAILBOX_DISPATCH_BUDGET.get()
                            || !scheduler.state().has_dispatch_capacity()
                        {
                            break Some(SchedulerTurn::Progress);
                        }
                    }
                }
                ReplyLane::Reply if scheduler.state().has_replies() => {
                    match scheduler.state().queue.poll(
                        &turn.inner.control,
                        turn.expected_mode,
                        task,
                    ) {
                        ReplyPoll::Pending => None,
                        ReplyPoll::Progress => Some(SchedulerTurn::Progress),
                        ReplyPoll::Leased => return Poll::Pending,
                        ReplyPoll::BudgetExhausted => {
                            scheduler.state().cursor = lane.next();
                            return Poll::Pending;
                        }
                    }
                }
                ReplyLane::ChildExit => poll_child(turn.access, task),
                _ => None,
            };

            if let Some(turn) = selected {
                scheduler.state().cursor = lane.next();
                return Poll::Ready(turn);
            }

            lane = lane.next();
            if lane == start {
                break;
            }
        }

        scheduler.state().cursor = start.next();
        Poll::Pending
    }

    fn clear(scheduler: &mut P, control: &Control) {
        scheduler.state().queue.clear(control);
    }
}

fn poll_child<A: Actor>(
    access: &mut ActorAccess<A>,
    task: &mut Context<'_>,
) -> Option<SchedulerTurn> {
    match access.state().poll_child_exit(task) {
        Poll::Ready(event) => Some(SchedulerTurn::Child(event)),
        Poll::Pending => None,
    }
}

#[cfg(test)]
mod tests;
