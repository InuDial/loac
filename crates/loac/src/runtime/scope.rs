use super::*;

// Runtime ownership stays private.
// Public scope views expose only phase-valid capabilities.
// Actor identity lives separately from mutable scope state.
// Pending `Cx` futures retain stable cell targets.
pub(crate) struct ScopeState<A: Actor> {
    pub(crate) children: <A as SupervisionConfig>::Children,
}

impl<A: Actor> ScopeState<A> {
    // This bridge keeps sealed profile details out of scheduler code.
    pub(crate) fn children(&mut self) -> &mut impl RuntimeChildren {
        self.children.__runtime(Seal)
    }

    /// Lends the capabilities valid before child cleanup.
    pub(crate) fn actor_scope<'a>(&'a mut self, actor_ref: &'a ActorRef<A>) -> ActorScope<'a, A> {
        ActorScope {
            actor_ref,
            state: self,
        }
    }

    /// Polls exits without exposing child storage to schedulers.
    pub(crate) fn poll_child_exit(&mut self, task: &mut Context<'_>) -> Poll<ChildExit> {
        self.children().poll_exit(task)
    }

    /// Lends the restricted cleanup capabilities.
    /// The exclusive borrow avoids requiring child state to be `Sync`.
    pub(crate) fn stop_scope<'a>(&'a mut self, actor_ref: &'a ActorRef<A>) -> StopScope<'a, A> {
        StopScope { actor_ref }
    }
}

/// Runtime capabilities available during [`Actor::on_stop`].
///
/// Child cleanup finishes before this capability is issued.
/// It cannot change the actor's child topology.
/// The runtime constructs this borrowed view after child cleanup.
/// The stop hook may keep it across `await`.
pub struct StopScope<'a, A: Actor> {
    actor_ref: &'a ActorRef<A>,
}

impl<A: Actor> StopScope<'_, A> {
    /// Returns this actor's non-owning address.
    ///
    /// Message admission is already closed during `on_stop`.
    #[must_use]
    pub const fn myself(&self) -> &ActorRef<A> {
        self.actor_ref
    }

    /// Requests shutdown while graceful cleanup is running.
    ///
    /// Stop or Drain has already committed at this stage.
    /// Kill may still upgrade either mode.
    #[must_use]
    pub fn request_shutdown(&self, shutdown: Shutdown) -> ShutdownStatus {
        self.actor_ref.request_shutdown(shutdown)
    }
}

impl<A: Actor> Deref for StopScope<'_, A> {
    type Target = ActorRef<A>;

    fn deref(&self) -> &Self::Target {
        self.actor_ref
    }
}

impl<A: Actor> fmt::Debug for StopScope<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StopScope")
            .field("actor_ref", self.myself())
            .finish_non_exhaustive()
    }
}

/// Runtime capabilities available before child cleanup begins.
///
/// The parent runtime owns every child actor. Actor state may keep a returned
/// [`Child`] or [`ActorRef`], but those values do not own the child.
/// Graceful shutdown waits for every retained child actor.
/// An unconfirmed descendant remains unconfirmed in the parent's final status.
/// [`Actor::on_stop`] receives [`StopScope`] instead.
///
/// The runtime constructs this borrowed view for user actor work.
/// `Cx::with` creates a fresh handler view for each call.
/// Initialization and lifecycle hooks may retain it across `await`.
pub struct ActorScope<'a, A: Actor> {
    pub(crate) actor_ref: &'a ActorRef<A>,
    pub(crate) state: &'a mut ScopeState<A>,
}

impl<A: Actor> ActorScope<'_, A> {
    /// Returns this actor's non-owning address.
    ///
    /// A handler's self-call needs another dispatch slot.
    /// A saturated scheduler cannot dispatch that self-call.
    /// A scheduler lease blocks its queued self-call.
    ///
    /// A serial lifecycle hook also blocks dispatch. While admission is still
    /// open, as in `init` or a running actor's `on_child_exit`, awaiting an
    /// accepted self-call likewise waits until Kill or executor teardown. Once
    /// shutdown closes admission, [`ActorRef::call`] returns
    /// [`CallError::Closed`](crate::CallError::Closed) and
    /// [`ActorRef::try_call`] reports
    /// [`TryCallErrorKind::Closed`](crate::TryCallErrorKind::Closed) instead.
    #[must_use]
    pub const fn myself(&self) -> &ActorRef<A> {
        self.actor_ref
    }

    /// Requests shutdown of this actor and, eventually, its subtree.
    ///
    /// This has the same first-wins and Kill-upgrade behavior as
    /// [`ActorOwner::request_shutdown`]. It commits synchronously, but Kill is
    /// cooperative: the current handler or poll returns before the runtime drops
    /// remaining actor work and propagates shutdown to children.
    ///
    /// Stop and Drain retain already-dispatched replies. Kill and reply
    /// completion instead commit through the same lifecycle gate, so whichever
    /// commits first determines the caller's result.
    #[must_use]
    pub fn request_shutdown(&self, shutdown: Shutdown) -> ShutdownStatus {
        self.actor_ref.request_shutdown(shutdown)
    }
}

impl<A: Actor> Deref for ActorScope<'_, A> {
    type Target = ActorRef<A>;

    fn deref(&self) -> &Self::Target {
        self.actor_ref
    }
}

impl<A: HasChildren> ActorScope<'_, A> {
    /// Spawns and owns one direct child actor.
    ///
    /// Child registration commits synchronously.
    /// Child initialization then runs asynchronously.
    /// Its mailbox accepts before initialization finishes.
    /// Registration completes before the child task can start.
    ///
    /// Finite profiles return [`Full`](crate::supervision::Full) at capacity.
    /// The error retains `args` without opening child configuration.
    /// Recover `args` through [`Full::into_inner`](crate::supervision::Full::into_inner).
    /// Unbounded profiles use [`Infallible`](std::convert::Infallible).
    /// Capacity counts retained direct-child registrations.
    /// A dequeued exit still retains its registration.
    /// Reaping releases capacity before [`Actor::on_child_exit`].
    ///
    /// The returned [`Child`] does not own lifecycle.
    /// Retained graceful work keeps the actor scope capability valid.
    /// A concurrent Kill cannot interrupt the current poll.
    pub fn spawn_child<C: Actor>(
        &mut self,
        args: C::SpawnArgs,
    ) -> Result<Child<C>, <A::Children as ChildSpawner>::Error<C::SpawnArgs>> {
        let args = self.state.children.__runtime_spawner(Seal).admit(args)?;
        let prepared = PreparedActor::new(args, SpawnOptions::<C>::default());
        let registered = self
            .state
            .children
            .__runtime_spawner(Seal)
            .register(prepared);
        Ok(start_child(registered))
    }

    /// Spawns and owns one direct child actor with explicit options.
    ///
    /// Registration and initialization follow [`spawn_child`](Self::spawn_child).
    ///
    /// A finite rejection retains `(args, options)`.
    /// [`Full::into_inner`](crate::supervision::Full::into_inner) returns that tuple.
    /// Child configuration remains unopened after rejection.
    ///
    /// The returned [`Child`] does not own lifecycle.
    /// Retained graceful work keeps the actor scope capability valid.
    /// A concurrent Kill cannot interrupt the current poll.
    #[allow(
        clippy::type_complexity,
        reason = "the error preserves both rejected spawn inputs"
    )]
    pub fn spawn_child_with<C: Actor>(
        &mut self,
        args: C::SpawnArgs,
        options: SpawnOptions<C>,
    ) -> Result<Child<C>, <A::Children as ChildSpawner>::Error<(C::SpawnArgs, SpawnOptions<C>)>>
    {
        let (args, options) = self
            .state
            .children
            .__runtime_spawner(Seal)
            .admit((args, options))?;
        let prepared = PreparedActor::new(args, options);
        let registered = self
            .state
            .children
            .__runtime_spawner(Seal)
            .register(prepared);
        Ok(start_child(registered))
    }
}

impl<A: Actor> fmt::Debug for ActorScope<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorScope")
            .field("actor_ref", self.myself())
            .finish_non_exhaustive()
    }
}
