#![allow(unsafe_code)]

use std::{cell::UnsafeCell, marker::PhantomPinned, pin::Pin, ptr::NonNull};

use super::*;

/// Owns stable actor storage after initialization completes.
///
/// Every mutable actor access originates from this cell.
/// Actor-aware operations never overlap.
/// Handler dispatch copies only a target pointer.
/// It never materializes a mutable actor reference.
/// Scheduled futures always drop before this cell.
/// Thus, retained `Cx` targets remain valid during destruction.
pub(crate) struct ActorAccess<A: Actor> {
    cell: Pin<Box<ActorCell<A>>>,
}

struct ActorCell<A: Actor> {
    actor_ref: ActorRef<A>,
    actor: UnsafeCell<A>,
    state: UnsafeCell<ScopeState<A>>,
    _pin: PhantomPinned,
}

/// Copyable access provenance for one stable actor cell.
pub(crate) struct CxTarget<A: Actor>(NonNull<ActorCell<A>>);

impl<A: Actor> Copy for CxTarget<A> {}

impl<A: Actor> Clone for CxTarget<A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A: Actor> ActorAccess<A> {
    pub(crate) fn new(actor_ref: ActorRef<A>, actor: A, state: ScopeState<A>) -> Self {
        Self {
            cell: Box::pin(ActorCell {
                actor_ref,
                actor: UnsafeCell::new(actor),
                state: UnsafeCell::new(state),
                _pin: PhantomPinned,
            }),
        }
    }

    /// Lends actor and scope access for one serialized runtime operation.
    pub(crate) fn parts(&mut self) -> (&mut A, ActorScope<'_, A>) {
        let cell = self.cell.as_ref().get_ref();
        // SAFETY: the actor task owns this cell. Callers never retain these
        // references while polling another actor operation.
        let actor = unsafe { &mut *cell.actor.get() };
        // SAFETY: actor state and scope state occupy separate cells.
        let state = unsafe { &mut *cell.state.get() };
        let scope = state.actor_scope(&cell.actor_ref);
        (actor, scope)
    }

    /// Lends actor and restricted cleanup capabilities.
    pub(crate) fn stop_parts(&mut self) -> (&mut A, StopScope<'_, A>) {
        let cell = self.cell.as_ref().get_ref();
        // SAFETY: cleanup excludes every other actor operation.
        let actor = unsafe { &mut *cell.actor.get() };
        // SAFETY: actor state and scope state occupy separate cells.
        let state = unsafe { &mut *cell.state.get() };
        let scope = state.stop_scope(&cell.actor_ref);
        (actor, scope)
    }

    /// Lends mutable scope state for one serialized runtime operation.
    pub(crate) fn state(&mut self) -> &mut ScopeState<A> {
        let cell = self.cell.as_ref().get_ref();
        // SAFETY: the actor task serializes every access to this cell.
        unsafe { &mut *cell.state.get() }
    }

    /// Runs one synchronous actor-only operation.
    pub(crate) fn with_actor<R>(&mut self, f: impl FnOnce(&mut A) -> R) -> R {
        let cell = self.cell.as_ref().get_ref();
        // SAFETY: the actor task serializes every access to this cell.
        f(unsafe { &mut *cell.actor.get() })
    }

    pub(crate) fn target(&self) -> CxTarget<A> {
        CxTarget(NonNull::from(self.cell.as_ref().get_ref()))
    }
}

impl<A: Actor> CxTarget<A> {
    /// Returns the target cell's immutable actor address.
    ///
    /// # Safety
    ///
    /// The target cell must remain alive during the returned borrow.
    pub(crate) unsafe fn actor_ref(&self) -> &ActorRef<A> {
        // SAFETY: the caller guarantees the target remains live.
        unsafe { &self.0.as_ref().actor_ref }
    }

    /// Lends both mutable cells through one synchronous closure.
    ///
    /// # Safety
    ///
    /// The target cell must remain alive throughout this call.
    /// No mutable actor access may overlap this call.
    pub(crate) unsafe fn with<R>(
        &mut self,
        f: impl for<'a> FnOnce(&'a mut A, &'a mut ActorScope<'a, A>) -> R,
    ) -> R {
        // SAFETY: the caller guarantees this target remains live.
        let cell = unsafe { self.0.as_ref() };
        // SAFETY: the caller guarantees unique actor access.
        let actor = unsafe { &mut *cell.actor.get() };
        // SAFETY: scope state occupies a separate stable cell.
        let state = unsafe { &mut *cell.state.get() };
        let mut scope = state.actor_scope(&cell.actor_ref);
        f(actor, &mut scope)
    }
}

// SAFETY: the actor task may move between threads while suspended.
// It never accesses the pointed cell concurrently from multiple threads.
unsafe impl<A: Actor> Send for CxTarget<A> {}
