//! What happens after user code panics while kanal holds the channel lock,
//! with kanal's own spin mutex (default) and with `std-mutex`.
//!
//! The only place kanal runs user code under the lock is `close()`, which
//! drops the queued items (`queue.clear()`) while holding it. A panicking
//! `Drop` there unwinds through the guard: the spin mutex just unlocks, while
//! `std::sync::Mutex` becomes poisoned. `acquire_internal` ignores poison
//! (`into_inner`), so every blocking / `try_*` operation behaves the same with
//! both mutexes. `try_acquire_internal` maps poison to "lock busy", so the
//! `*_realtime` methods differ: with std-mutex they report "nothing done"
//! (`Ok(None)` / `Ok(false)`) forever instead of the closed error.

use kanal::{unbounded, ReceiveError, SendError};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Panics on drop when armed.
struct Bomb(bool);
impl Drop for Bomb {
    fn drop(&mut self) {
        if self.0 && !std::thread::panicking() {
            panic!("Bomb dropped under the channel lock");
        }
    }
}

fn close_with_panicking_drop() -> (kanal::Sender<Bomb>, kanal::Receiver<Bomb>) {
    let (s, r) = unbounded::<Bomb>();
    s.send(Bomb(true)).unwrap();
    s.send(Bomb(false)).unwrap();
    let res = catch_unwind(AssertUnwindSafe(|| {
        let _ = s.close();
    }));
    assert!(res.is_err(), "close() should propagate the Drop panic");
    (s, r)
}

#[test]
fn blocking_and_try_ops_after_panic_under_lock_are_identical() {
    let (s, r) = close_with_panicking_drop();
    // state is consistent: closed and empty, for both mutexes
    assert!(s.is_closed());
    assert!(r.is_closed());
    assert_eq!(r.len(), 0);
    assert!(matches!(r.try_recv(), Err(ReceiveError::Closed)));
    assert!(matches!(r.recv(), Err(ReceiveError::Closed)));
    assert!(matches!(
        r.recv_timeout(std::time::Duration::from_millis(10)),
        Err(kanal::ReceiveErrorTimeout::Closed)
    ));
    assert!(matches!(s.try_send(Bomb(false)), Err(SendError::Closed)));
    assert!(matches!(s.send(Bomb(false)), Err(SendError::Closed)));
    // a fresh channel is unaffected
    let (s2, r2) = unbounded::<u32>();
    s2.send(1).unwrap();
    assert_eq!(r2.try_recv().unwrap(), Some(1));
}

#[test]
fn realtime_ops_after_panic_under_lock() {
    let (s, r) = close_with_panicking_drop();
    let rr = r.try_recv_realtime();
    let sr = s.try_send_realtime(Bomb(false));
    #[cfg(not(feature = "std-mutex"))]
    {
        assert!(matches!(rr, Err(ReceiveError::Closed)));
        assert!(matches!(sr, Err(SendError::Closed)));
    }
    #[cfg(feature = "std-mutex")]
    {
        // poisoned lock looks permanently busy to try_lock
        assert!(matches!(rr, Ok(None)));
        assert!(matches!(sr, Ok(false)));
    }
}
