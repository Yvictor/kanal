use cacheguard::CacheGuard;
use core::sync::atomic::{AtomicBool, Ordering};
use lock_api::{GuardSend, RawMutex};

use crate::backoff::*;
#[cfg_attr(feature = "parking-lot", allow(dead_code))]
pub struct RawMutexLock {
    locked: CacheGuard<AtomicBool>,
}

#[cfg_attr(feature = "parking-lot", allow(dead_code))]
impl RawMutexLock {
    #[inline(never)]
    fn lock_no_inline(&self) {
        #[cfg(feature = "nosleep-spin")]
        spin_cond_no_sleep(|| self.try_lock());
        #[cfg(not(feature = "nosleep-spin"))]
        spin_cond(|| self.try_lock());
    }
}

unsafe impl RawMutex for RawMutexLock {
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: RawMutexLock = RawMutexLock {
        locked: CacheGuard::new(AtomicBool::new(false)),
    };
    type GuardMarker = GuardSend;
    #[inline(always)]
    fn lock(&self) {
        if self.try_lock() {
            return;
        }
        self.lock_no_inline();
    }

    #[inline(always)]
    fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline(always)]
    unsafe fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}
#[allow(dead_code)]
#[cfg(not(feature = "parking-lot"))]
pub type Mutex<T> = lock_api::Mutex<RawMutexLock, T>;
#[cfg(not(any(feature = "std-mutex", feature = "parking-lot")))]
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, RawMutexLock, T>;

// Feature `parking-lot`: parking_lot's adaptive mutex (spin briefly, then
// park; unlock wakes one waiter; eventual fairness).
#[cfg(feature = "parking-lot")]
pub type Mutex<T> = lock_api::Mutex<parking_lot::RawMutex, T>;
#[cfg(all(feature = "parking-lot", not(feature = "std-mutex")))]
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, parking_lot::RawMutex, T>;
