//! Scenario 5: panicking payload Drop and panicking wakers, then continued use.

use crate::common::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Wake, Waker};

fn quiet<R>(f: impl FnOnce() -> R) -> std::thread::Result<R> {
    catch_unwind(AssertUnwindSafe(f))
}

fn works(tx: &flume::Sender<Small>, rx: &flume::Receiver<Small>, ctr: &'static Counters, id: u64) -> bool {
    quiet(|| {
        tx.send(Msg::new(id, ctr)).is_ok() && rx.try_recv().map(|m| m.id) == Ok(id)
    })
    .unwrap_or(false)
}

/// Payload Drop panics in every place a payload can be dropped.
pub fn payload_drop() -> Outcome {
    let ctr = Counters::leak();
    ctr.panic_mod.store(7, SeqCst); // ids divisible by 7 panic on drop (except id 0 below)
    let mut r = vec![];
    let mut ok = true;
    let mut note = |name: &str, cond: bool| {
        ok &= cond;
        r.push(format!("{name}:{}", if cond { "ok" } else { "FAILED" }));
    };
    let (tx, rx) = flume::unbounded::<Small>();
    // (1) received message dropped by user code
    let mut panics = 0;
    for id in 1..=70 {
        tx.send(Msg::new(id, ctr)).unwrap();
        let m = rx.recv().unwrap();
        if quiet(move || drop(m)).is_err() {
            panics += 1;
        }
    }
    note("recv-then-drop(10 panics)", panics == 10 && works(&tx, &rx, ctr, 1006));
    // (2) Drain iterator dropped with a panicking element still inside
    for id in [1002, 1003, 1008, 1004] {
        tx.send(Msg::new(id, ctr)).unwrap(); // 1008 % 7 == 0
    }
    let d = quiet(|| drop(rx.drain())).is_err();
    note("drain-drop", d && works(&tx, &rx, ctr, 1005));
    // (3) cancelled send_async holding a panicking payload
    {
        let (btx, brx) = flume::bounded::<Small>(1);
        btx.send(Msg::new(2001, ctr)).unwrap();
        let (_c, w) = count_waker();
        let mut f = btx.send_async(Msg::new(2002, ctr)); // 2002 % 7 == 0
        assert!(poll_once(&mut f, &w).is_pending());
        let p = quiet(move || drop(f)).is_err();
        let a = quiet(|| brx.try_recv().map(|m| m.id)).ok();
        let still = quiet(|| brx.try_recv().is_err()).unwrap_or(false);
        let b = quiet(|| btx.try_send(Msg::new(2003, ctr)).is_ok()).unwrap_or(false);
        note(
            "cancelled-send_async-drop",
            p && a == Some(Ok(2001)) && still && b && brx.try_recv().map(|m| m.id) == Ok(2003),
        );
    }
    // (4) send_timeout timing out on a full bounded channel, item returned and dropped by user
    {
        let (btx, brx) = flume::bounded::<Small>(1);
        btx.send(Msg::new(3001, ctr)).unwrap();
        let e = btx.send_timeout(Msg::new(3003, ctr), std::time::Duration::from_millis(5)); // 3003 % 7 == 0
        let p = quiet(move || drop(e)).is_err();
        note("send_timeout-returned-item-drop", p && brx.try_recv().map(|m| m.id) == Ok(3001));
        let ok2 = works(&btx, &brx, ctr, 3002);
        note("bounded-still-works", ok2);
    }
    // (5) whole channel dropped with a panicking payload queued
    for id in [4001, 4004, 4002] {
        tx.send(Msg::new(id, ctr)).unwrap(); // 4004 % 7 == 0
    }
    let p = quiet(move || {
        drop(tx);
        drop(rx);
    })
    .is_err();
    note("channel-drop-with-queued", p);
    let leak = ctr.live();
    note("no-leak", leak == 0);
    Outcome::new("S5a panicking payload Drop", ok, r.join(" "))
}

struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("waker panic");
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("waker panic");
    }
}

fn clone_panics_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        panic!("waker clone panic")
    }
    unsafe fn noop(_: *const ()) {}
    static VT: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VT)) }
}

fn usable(tx: &flume::Sender<Small>, rx: &flume::Receiver<Small>, ctr: &'static Counters) -> &'static str {
    match quiet(|| tx.try_send(Msg::new(99, ctr)).is_ok() && rx.try_recv().is_ok()) {
        Ok(true) => "usable",
        Ok(false) => "errors",
        Err(_) => "POISONED(panics)",
    }
}

/// Panicking wakers: wake panic on recv path, clone panic, wake panic on send path,
/// wake panic on disconnect.
pub fn panicking_waker() -> Outcome {
    let ctr = Counters::leak();
    let mut r = vec![];
    // (1) recv_async registered with a waker whose wake panics; a sender fires it
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let w = Waker::from(Arc::new(PanicWake));
        let mut f = rx.recv_async();
        assert!(poll_once(&mut f, &w).is_pending());
        let send_panicked = quiet(|| tx.send(Msg::new(1, ctr))).is_err();
        let state = usable(&tx, &rx, ctr);
        let drop_f = quiet(move || drop(f)).is_err();
        let drop_tx = quiet(move || drop(tx)).is_err();
        let drop_rx = quiet(move || drop(rx)).is_err();
        r.push(format!(
            "wake-panic-on-send: send panicked={send_panicked}, then channel {state}, drop(fut) panicked={drop_f}, drop(tx) panicked={drop_tx}, drop(rx) panicked={drop_rx}"
        ));
    }
    // (2) waker whose clone panics, used to poll recv_async
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let w = clone_panics_waker();
        let mut f = rx.recv_async();
        let p = quiet(|| poll_once(&mut f, &w).is_pending()).is_err();
        let drop_f = quiet(move || drop(f)).is_err();
        let state = usable(&tx, &rx, ctr);
        // drop endpoints separately: dropping both in one scope while poisoned is a
        // panic-during-unwind => process abort (observed)
        let d = quiet(move || drop(tx)).is_err() | quiet(move || drop(rx)).is_err();
        r.push(format!("clone-panic-on-poll: poll panicked={p}, then channel {state}, drop(fut) panicked={drop_f}, drop endpoints panicked={d}"));
    }
    // (3) send_async parked on full bounded channel with a panicking waker; receiver pulls
    {
        let (tx, rx) = flume::bounded::<Small>(1);
        tx.send(Msg::new(10, ctr)).unwrap();
        let w = Waker::from(Arc::new(PanicWake));
        let mut f = tx.clone().into_send_async(Msg::new(11, ctr));
        assert!(poll_once(&mut f, &w).is_pending());
        let p = quiet(|| rx.try_recv().is_ok()).is_err();
        let state = usable(&tx, &rx, ctr);
        let d = quiet(move || drop(f)).is_err()
            | quiet(move || drop(tx)).is_err()
            | quiet(move || drop(rx)).is_err();
        r.push(format!("wake-panic-on-recv-pull: try_recv panicked={p}, then channel {state}, drops panicked={d}"));
    }
    // (4) recv_async with panicking waker; last sender dropped
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let w = Waker::from(Arc::new(PanicWake));
        let mut f = rx.clone().into_recv_async();
        assert!(poll_once(&mut f, &w).is_pending());
        let p = quiet(move || drop(tx)).is_err();
        let rstate = quiet(|| rx.try_recv()).map(|x| format!("{x:?}")).unwrap_or("POISONED(panics)".into());
        let d = quiet(move || drop(f)).is_err() | quiet(move || drop(rx)).is_err();
        r.push(format!("wake-panic-on-disconnect: drop(last tx) panicked={p}, rx.try_recv -> {rstate}, drops panicked={d}"));
    }
    // A channel poisoned by a panicking waker cannot be used again; flag it as a finding
    // (FAIL) only if the process aborted, which we would not reach. Report as characterisation.
    r.push("(dropping two poisoned endpoints in one scope aborts the process: panic in Drop during unwind)".into());
    let poisoned = r.iter().any(|s| s.contains("POISONED"));
    Outcome::repro("S5b panicking waker (characterisation)", poisoned, r.join(" ; "))
}
