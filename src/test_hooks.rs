//! Test-only instrumentation of the channel's handoff protocol.
//!
//! The library calls [`hit`] (through the `test_hook!` macro) at the points
//! where a race between a waiter and its peer can happen. A test installs a
//! per-thread hook with [`set`] and uses it to hold a thread at that point
//! until another thread has done its part, so the interleaving is fixed by the
//! test instead of by the scheduler. This module and every call into it only
//! exist under `cfg(test)`.

use std::cell::RefCell;
use std::time::Instant;

/// Instrumented points.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hook {
    /// A peer took a waiter's signal out of the wait list and released the
    /// channel lock, but has not completed the signal yet.
    Handoff,
    /// A sync timeout waiter pushed its signal and released the lock, right
    /// before `wait_timeout`. The hook may return a replacement deadline.
    Parked,
    /// `wait_timeout` of a sync timeout waiter returned false, before the
    /// waiter checks for termination and tries to cancel its signal.
    TimedOut,
    /// `wait_after_timeout` saw the signal still owned by the peer and is
    /// about to park (called on every iteration).
    AwaitPeerPark,
    /// `async_blocking_wait` saw the signal still owned by the peer and is
    /// about to wait for it.
    AwaitPeerAsync,
    /// A repolled future found its signal still queued and is about to
    /// replace the registered waker.
    WakerSwap,
}

type HookFn = Box<dyn FnMut(Hook) -> Option<Instant>>;

thread_local! {
    static HOOK: RefCell<Option<HookFn>> = const { RefCell::new(None) };
}

/// Runs the current thread's hook, if any. The hook is taken out while it
/// runs, so channel operations performed by the hook itself do not re-enter
/// it.
pub(crate) fn hit(point: Hook) -> Option<Instant> {
    let hook = HOOK.try_with(|h| h.borrow_mut().take()).ok().flatten();
    let mut hook = hook?;
    let ret = hook(point);
    let _ = HOOK.try_with(|h| {
        let mut h = h.borrow_mut();
        if h.is_none() {
            *h = Some(hook);
        }
    });
    ret
}

/// `Hook::Parked`: lets the test replace the waiter's deadline.
pub(crate) fn parked(deadline: Instant) -> Instant {
    hit(Hook::Parked).unwrap_or(deadline)
}

/// Removes the hook it was returned for when dropped.
pub(crate) struct HookGuard(());

impl Drop for HookGuard {
    fn drop(&mut self) {
        let _ = HOOK.try_with(|h| h.borrow_mut().take());
    }
}

/// Installs `f` as the current thread's hook until the guard is dropped.
pub(crate) fn set(f: impl FnMut(Hook) -> Option<Instant> + 'static) -> HookGuard {
    HOOK.with(|h| *h.borrow_mut() = Some(Box::new(f)));
    HookGuard(())
}
