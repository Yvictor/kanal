//! Reproductions of defects found by reading flume 0.12.0.
//!
//! BUG-1 (lost wakeup of sync receivers): `RecvFut::reset_hook` (async.rs:388-407), run when
//! a `recv_async` future that was ever woken is dropped -- cancelled, OR simply dropped after
//! completing normally (the hook is not cleared on Ready, async.rs:415-417) -- calls
//! `Chan::try_wake_receiver_if_pending` (lib.rs:464-468). That pops waiters from
//! `chan.waiting` and fires them while `fire()` returns `false` -- i.e. it pops *every*
//! non-stream waiter, not just one. For a sync receiver (`recv`, `recv_timeout`) firing only
//! unparks the thread; its slot is still empty so `wait_recv`/`wait_deadline_recv`
//! (lib.rs:342-371) parks again, but its hook is no longer in `waiting`, so no later send
//! will ever hand it a message or wake it. The message sits in the queue until the sync
//! receiver's deadline (recv_timeout) or forever / until disconnect (recv).
//!
//! BUG-2 (stale duplicate async hooks): `AsyncSignal::woken` (async.rs:21) is set by `fire`
//! and never cleared, so after a recv_async future has been woken once, *every* later
//! Pending poll re-pushes its hook (async.rs:425-431) even if it is already queued. The
//! `waiting` deque grows by one entry per poll until the future completes or is dropped.

use crate::common::*;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// BUG-1 with recv_timeout: returns the delay the sync consumer saw for a message that
/// was sitting in the queue.
pub fn orphaned_recv_timeout(wait: Duration, settle: Duration) -> Outcome {
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Small>();
    let (_c, w) = count_waker();
    let mut a = rx.recv_async();
    assert!(poll_once(&mut a, &w).is_pending()); // async receiver registered first
    let rx2 = rx.clone();
    let h = std::thread::spawn(move || {
        let r = rx2.recv_timeout(wait);
        (r.map(|m| m.id), Instant::now())
    });
    std::thread::sleep(settle); // sync receiver parks, its hook queued behind A's
    let t_send = Instant::now();
    tx.send(Msg::new(1, ctr)).unwrap(); // pops A, message queued, A woken
    drop(a); // A cancelled before being polled again
    let (r, t_got) = h.join().unwrap();
    let delay = t_got - t_send;
    let reproduced = r == Ok(1) && delay > wait / 2;
    Outcome::repro(
        "BUG-1a cancelled woken recv_async orphans parked recv_timeout",
        reproduced,
        format!(
            "recv_timeout({wait:?}) returned {r:?} {:.1} ms after the send (expected ~0 ms); queue held the message the whole time",
            delay.as_secs_f64() * 1e3
        ),
    )
}

/// BUG-1 without any cancellation: two recv_async futures complete normally during a
/// 2-message burst; dropping the first (after it returned its message) orphans the
/// parked sync receiver.
pub fn orphaned_no_cancel(wait: Duration, settle: Duration) -> Outcome {
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Small>();
    let (_ca, wa) = count_waker();
    let (_cb, wb) = count_waker();
    let mut a = rx.recv_async();
    let mut b = rx.recv_async();
    assert!(poll_once(&mut a, &wa).is_pending());
    assert!(poll_once(&mut b, &wb).is_pending());
    let rx2 = rx.clone();
    let h = std::thread::spawn(move || {
        let r = rx2.recv_timeout(wait);
        (r.map(|m| m.id), Instant::now())
    });
    std::thread::sleep(settle); // waiting = [A, B, C]
    tx.send(Msg::new(1, ctr)).unwrap(); // A woken, queue [1]
    tx.send(Msg::new(2, ctr)).unwrap(); // B woken, queue [1, 2]
    let ra = poll_once(&mut a, &wa); // A -> Ready(1)
    drop(a); // normal completion; queue still [2] -> pops and fires B and C
    let rb = poll_once(&mut b, &wb); // B -> Ready(2)
    drop(b);
    let t_send = Instant::now();
    tx.send(Msg::new(3, ctr)).unwrap(); // C should get this immediately
    let (r, t_got) = h.join().unwrap();
    let delay = t_got - t_send;
    let ok_async = matches!(ra, std::task::Poll::Ready(Ok(ref m)) if m.id == 1)
        && matches!(rb, std::task::Poll::Ready(Ok(ref m)) if m.id == 2);
    Outcome::repro(
        "BUG-1d no cancellation: 2 recv_async complete normally in a burst, parked recv_timeout orphaned",
        ok_async && r == Ok(3) && delay > wait / 2,
        format!(
            "async futures got 1,2 normally; recv_timeout({wait:?}) then got msg 3 only {:.1} ms after it was sent (expected ~0 ms)",
            delay.as_secs_f64() * 1e3
        ),
    )
}

/// BUG-1 with blocking recv(): the consumer stays parked while messages pile up, and is
/// not even woken when the last sender is dropped (its hook is not in `waiting`, so
/// `disconnect_all` does not unpark it) -> permanent hang. The stuck thread is leaked.
pub fn orphaned_recv(settle: Duration) -> Outcome {
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Small>();
    let (_c, w) = count_waker();
    let mut a = rx.recv_async();
    assert!(poll_once(&mut a, &w).is_pending());
    let rx2 = rx.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let r = rx2.recv().map(|m| m.id);
        let _ = done_tx.send(r);
    });
    std::thread::sleep(settle);
    tx.send(Msg::new(1, ctr)).unwrap();
    drop(a);
    std::thread::sleep(settle);
    tx.send(Msg::new(2, ctr)).unwrap();
    tx.send(Msg::new(3, ctr)).unwrap();
    let before = done_rx.recv_timeout(settle);
    let queued = rx.len();
    drop(tx); // all senders gone: a correct channel returns Ok(1) now
    let after = match before {
        Ok(r) => Some(r),
        Err(_) => done_rx.recv_timeout(Duration::from_secs(3)).ok(),
    };
    let stuck = before.is_err();
    let detail = match (&before, &after) {
        (Ok(r), _) => format!("recv() returned {r:?} promptly"),
        (Err(_), Some(r)) => format!(
            "recv() parked {:?} with {queued} msgs queued; returned {r:?} only after the last sender was dropped",
            settle * 2
        ),
        (Err(_), None) => format!(
            "recv() parked {:?} with {queued} msgs queued and STILL parked 3 s after the last sender was dropped (permanent hang; thread leaked)",
            settle * 2
        ),
    };
    std::mem::forget(rx);
    Outcome::repro("BUG-1b cancelled woken recv_async orphans parked recv()", stuck, detail)
}

/// BUG-1 in a realistic shape: two OS-thread `recv_timeout(100ms)` workers and one async
/// consumer task (`recv_async` loop, like a Python asyncio subscriber) share a channel.
/// Each round the async consumer is cancelled for good (task aborted, e.g. unsubscribe /
/// asyncio cancellation) -- either right after a message was sent (`racy`) or while idle
/// (control) -- and traffic continues at 1 msg / 2 ms for 150 ms. Records the sync
/// workers' latency after the cancellation.
pub fn orphaned_realistic(rounds: u64, racy: bool) -> Outcome {
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Small>();
    let rt = tokio_rt(2);
    let lat_after = Arc::new(Lat::default());
    let phase_after = Arc::new(AtomicBool::new(false));
    let mut hs = vec![];
    for _ in 0..2 {
        let (rx, lat, ph) = (rx.clone(), lat_after.clone(), phase_after.clone());
        hs.push(std::thread::spawn(move || loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(m) => {
                    if ph.load(SeqCst) {
                        lat.record(now_ns() - m.ts)
                    }
                }
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(_) => break,
            }
        }));
    }
    let mut id = 0;
    let mut rng = Rng::new(9);
    let mut bad_rounds = 0;
    for _ in 0..rounds {
        phase_after.store(false, SeqCst);
        let rxa = rx.clone();
        let task = rt.spawn(async move { while rxa.recv_async().await.is_ok() {} });
        for _ in 0..5 + rng.below(5) {
            tx.send(Msg::new(id, ctr)).unwrap();
            id += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        let before = lat_after.over50ms.load(SeqCst);
        if racy {
            tx.send(Msg::new(id, ctr)).unwrap();
            id += 1;
            task.abort();
        } else {
            task.abort();
            let _ = rt.block_on(task);
            tx.send(Msg::new(id, ctr)).unwrap();
            id += 1;
        }
        phase_after.store(true, SeqCst);
        for _ in 0..75 {
            std::thread::sleep(Duration::from_millis(2));
            tx.send(Msg::new(id, ctr)).unwrap();
            id += 1;
        }
        std::thread::sleep(Duration::from_millis(120));
        if lat_after.over50ms.load(SeqCst) > before {
            bad_rounds += 1;
        }
    }
    drop(tx);
    for h in hs {
        h.join().unwrap();
    }
    drop(rt);
    let name = if racy {
        "BUG-1c realistic: async consumer cancelled as a msg arrives, 2 recv_timeout workers"
    } else {
        "BUG-1c control: async consumer cancelled while idle, 2 recv_timeout workers"
    };
    Outcome::repro(
        name,
        bad_rounds > 0,
        format!(
            "{rounds} rounds, {bad_rounds} rounds where sync workers stalled >50ms with msgs queued; sync lat after cancel: {}",
            lat_after.describe()
        ),
    )
}

/// BUG-2: after one wake whose message was stolen, every Pending repoll adds another copy
/// of the hook to `waiting`. Measured as: how many sends are routed to the stale future
/// before a receiver registered after it is woken.
pub fn sticky_woken(repolls: u64) -> Outcome {
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Small>();
    let (_ca, wa) = count_waker();
    let mut a = rx.recv_async();
    assert!(poll_once(&mut a, &wa).is_pending());
    tx.send(Msg::new(0, ctr)).unwrap(); // wakes A
    let _ = rx.try_recv().unwrap(); // stolen by a competing receiver
    for _ in 0..repolls {
        assert!(poll_once(&mut a, &wa).is_pending());
    }
    let (cb, wb) = count_waker();
    let mut b = rx.recv_async();
    assert!(poll_once(&mut b, &wb).is_pending());
    let mut sends_before_b = 0u64;
    let limit = repolls + 10;
    while cb.0.load(SeqCst) == 0 && sends_before_b < limit {
        tx.send(Msg::new(1 + sends_before_b, ctr)).unwrap();
        sends_before_b += 1;
    }
    let queued = rx.len();
    drop(a);
    drop(b);
    drop((tx, rx));
    Outcome::repro(
        "BUG-2 sticky `woken`: duplicate hooks per repoll",
        sends_before_b > 2,
        format!(
            "after {repolls} Pending repolls of one stale recv_async future, {sends_before_b} sends were all routed to it (B, registered later, not woken; {queued} msgs left queued) => waiting deque held ~{} entries for one future",
            sends_before_b - 1
        ),
    )
}
