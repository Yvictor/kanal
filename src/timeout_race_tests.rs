//! Deterministic tests for the races between a waiter whose deadline expires
//! (or whose future is dropped) and a peer that already took its signal.
//!
//! The interleavings are fixed with the test-only hooks in `test_hooks`: the
//! peer is held right after it took the waiter's signal, the waiter's deadline
//! is expired on demand, and the peer is only released once the waiter has
//! reached the point under test. Nothing depends on how fast threads are
//! scheduled. Every wait on a gate is bounded by `GATE_TIMEOUT`; a gate that
//! times out lets the blocked thread carry on and fails the test.
//!
//! Every test lives in this module, so `cargo miri test --lib timeout_race`
//! runs all of them (the stress test is skipped under Miri).

use super::*;
use crate::test_hooks::{self, Hook};
use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

/// Upper bound for any wait on a gate. Only reached when the code under test
/// does not follow the planned interleaving.
const GATE_TIMEOUT: Duration = Duration::from_secs(if cfg!(miri) { 600 } else { 60 });
/// Timeout given to the timed operations under test. Tests expire the
/// deadline themselves through `Hook::Parked`; this only has to outlast the
/// time between the call and the waiter parking.
const OP_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------------

/// One-shot event that threads can wait for, with a bounded wait.
#[derive(Clone, Default)]
struct Flag(Arc<(Mutex<bool>, Condvar)>);

impl Flag {
    fn raise(&self) {
        let (lock, cvar) = &*self.0;
        *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
        cvar.notify_all();
    }

    fn is_raised(&self) -> bool {
        *self.0 .0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Waits until raised; false if `GATE_TIMEOUT` passed first.
    fn wait(&self) -> bool {
        let (lock, cvar) = &*self.0;
        let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = cvar
            .wait_timeout_while(guard, GATE_TIMEOUT, |raised| !*raised)
            .unwrap_or_else(|e| e.into_inner());
        *guard
    }

    #[track_caller]
    fn expect(&self, what: &str) {
        assert!(
            self.wait(),
            "timed out after {GATE_TIMEOUT:?} waiting for: {what}"
        );
    }
}

/// Interleavings of a timed waiter (`recv_timeout`, `send_timeout`,
/// `send_option_timeout`) and the peer that serves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// The peer takes the parked waiter's signal and is held. The deadline
    /// then expires, the waiter fails to cancel, and the peer is released
    /// only once the waiter is waiting for it. The waiter must succeed.
    HeldAcrossDeadline,
    /// The deadline expires first; before the waiter tries to cancel, the
    /// peer takes the signal and is held until the waiter waits for it. The
    /// waiter must succeed.
    TakenAfterDeadline,
    /// The deadline expires; the peer takes and completes the signal before
    /// the waiter tries to cancel. The waiter must succeed.
    CompletedAfterDeadline,
    /// The deadline expires and the waiter cancels before the peer acts; the
    /// peer runs after the waiter returned. The waiter must time out.
    CancelledFirst,
    /// `between` runs while the waiter is parked and before any peer acted
    /// (e.g. `close()`); the peer runs after the waiter returned.
    BeforePeer,
}

#[derive(Clone, Default)]
struct Script {
    /// The waiter pushed its signal and released the lock.
    parked: Flag,
    /// The waiter may let its deadline expire.
    may_expire: Flag,
    /// The waiter's `wait_timeout` returned false.
    timed_out: Flag,
    /// The waiter may check termination and try to cancel.
    may_check: Flag,
    /// The peer took the waiter's signal.
    acquired: Flag,
    /// The peer may complete the signal.
    release: Flag,
    /// The waiter blocked on a signal owned by the peer.
    waited: Flag,
    peer_done: Flag,
    waiter_done: Flag,
    /// Some gate wait timed out.
    gate_expired: Flag,
}

impl Script {
    fn hold(&self, flag: &Flag) {
        if !flag.wait() {
            self.gate_expired.raise();
        }
    }

    fn open_all(&self) {
        for f in [
            &self.may_expire,
            &self.may_check,
            &self.release,
            &self.peer_done,
        ] {
            f.raise();
        }
    }

    fn waiter_hook(&self, plan: Plan) -> impl FnMut(Hook) -> Option<Instant> + 'static {
        let s = self.clone();
        move |point| {
            match point {
                Hook::Parked => {
                    s.parked.raise();
                    if matches!(plan, Plan::HeldAcrossDeadline | Plan::BeforePeer) {
                        s.hold(&s.may_expire);
                    }
                    // Expire the deadline right now.
                    return Some(Instant::now());
                }
                Hook::TimedOut => {
                    s.timed_out.raise();
                    match plan {
                        Plan::TakenAfterDeadline => s.hold(&s.may_check),
                        Plan::CompletedAfterDeadline => s.hold(&s.peer_done),
                        _ => {}
                    }
                }
                Hook::AwaitPeerPark | Hook::AwaitPeerAsync => {
                    // The waiter is now waiting for the held peer: let it go.
                    s.waited.raise();
                    s.release.raise();
                }
                Hook::Handoff | Hook::WakerSwap => {}
            }
            None
        }
    }

    fn peer_hook(&self, plan: Plan) -> impl FnMut(Hook) -> Option<Instant> + 'static {
        let s = self.clone();
        let mut first = true;
        move |point| {
            if point == Hook::Handoff && first {
                first = false;
                s.acquired.raise();
                if matches!(plan, Plan::HeldAcrossDeadline | Plan::TakenAfterDeadline) {
                    s.hold(&s.release);
                }
            }
            None
        }
    }
}

/// Opens every gate if the controlling thread unwinds, so no helper thread
/// is left waiting for the full gate timeout.
struct OpenOnPanic<'a>(&'a Script);

impl Drop for OpenOnPanic<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.open_all();
        }
    }
}

struct Outcome<W, P> {
    waiter: W,
    peer: P,
}

fn join<R>(h: thread::ScopedJoinHandle<'_, R>) -> R {
    h.join().unwrap_or_else(|e| resume_unwind(e))
}

/// Runs `waiter` and `peer` on their own threads in the interleaving given
/// by `plan`; `between` runs on the calling thread at the plan's checkpoint.
fn run_race<W: Send, P: Send>(
    plan: Plan,
    waiter: impl FnOnce() -> W + Send,
    peer: impl FnOnce() -> P + Send,
    between: impl FnOnce(),
) -> Outcome<W, P> {
    let script = Script::default();
    let s = &script;
    let (waiter, peer) = thread::scope(|scope| {
        let _open = OpenOnPanic(s);
        let w = scope.spawn(move || {
            let _hook = test_hooks::set(s.waiter_hook(plan));
            let r = waiter();
            s.waiter_done.raise();
            s.release.raise();
            r
        });
        let p = scope.spawn(move || {
            let _hook = test_hooks::set(s.peer_hook(plan));
            s.hold(match plan {
                Plan::HeldAcrossDeadline => &s.parked,
                Plan::TakenAfterDeadline | Plan::CompletedAfterDeadline => &s.timed_out,
                Plan::CancelledFirst | Plan::BeforePeer => &s.waiter_done,
            });
            let r = peer();
            s.peer_done.raise();
            r
        });
        match plan {
            Plan::HeldAcrossDeadline => {
                s.acquired.expect("the peer to take the waiter's signal");
                between();
                s.may_expire.raise();
            }
            Plan::TakenAfterDeadline => {
                s.acquired
                    .expect("the peer to take the timed-out waiter's signal");
                between();
                s.may_check.raise();
            }
            Plan::BeforePeer => {
                s.parked.expect("the waiter to park");
                between();
                s.may_expire.raise();
            }
            Plan::CompletedAfterDeadline | Plan::CancelledFirst => between(),
        }
        let waiter = join(w);
        let peer = join(p);
        (waiter, peer)
    });
    assert!(
        !script.gate_expired.is_raised(),
        "{plan:?}: a gate wait timed out, the planned interleaving did not happen"
    );
    if matches!(plan, Plan::HeldAcrossDeadline | Plan::TakenAfterDeadline) {
        assert!(
            script.waited.is_raised(),
            "{plan:?}: the waiter returned without waiting for the peer that owned its signal"
        );
    }
    Outcome { waiter, peer }
}

// ---------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------

const SLOTS: usize = 8192;
static DROPS: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];
/// Slot 0 is shared by all `Zst` values; tests using it are serialized.
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(1);
static ZST_SERIAL: Mutex<()> = Mutex::new(());

/// Drop counter of one test.
struct Probe {
    slot: usize,
    _serial: Option<MutexGuard<'static, ()>>,
}

impl Probe {
    fn new() -> Self {
        let slot = NEXT_SLOT.fetch_add(1, SeqCst);
        assert!(slot < SLOTS, "out of drop counters");
        Probe {
            slot,
            _serial: None,
        }
    }

    fn drops(&self) -> usize {
        DROPS[self.slot].load(SeqCst)
    }
}

fn count_drop(slot: usize) {
    DROPS[slot].fetch_add(1, SeqCst);
}

/// Payload kinds moved differently by `KanalPtr`.
trait Payload: Send + Sized + fmt::Debug + 'static {
    const KIND: &'static str;
    fn probe() -> Probe {
        Probe::new()
    }
    fn make(probe: &Probe, id: u8) -> Self;
    /// The id carried by the value, `None` for kinds that carry none.
    fn id(&self) -> Option<u8>;
    fn tag(id: u8) -> Option<u8> {
        Some(id)
    }
}

/// Larger than a pointer: moved through a pointer to the waiter's storage.
#[derive(Debug)]
struct Big {
    slot: u32,
    id: u8,
    _pad: [u64; 3],
}

impl Drop for Big {
    fn drop(&mut self) {
        count_drop(self.slot as usize);
    }
}

impl Payload for Big {
    const KIND: &'static str = "larger than a pointer";
    fn make(probe: &Probe, id: u8) -> Self {
        Big {
            slot: probe.slot as u32,
            id,
            _pad: [0; 3],
        }
    }
    fn id(&self) -> Option<u8> {
        Some(self.id)
    }
}

/// Exactly pointer sized and owning a heap allocation (a double drop is a
/// double free).
#[derive(Debug)]
struct Boxed(Box<Big>);

impl Payload for Boxed {
    const KIND: &'static str = "pointer sized, owning";
    fn make(probe: &Probe, id: u8) -> Self {
        Boxed(Box::new(Big::make(probe, id)))
    }
    fn id(&self) -> Option<u8> {
        Some(self.0.id)
    }
}

/// Smaller than a pointer, with Drop: stored inline in the signal pointer.
#[derive(Debug)]
struct Small {
    slot: u16,
    id: u8,
}

impl Drop for Small {
    fn drop(&mut self) {
        count_drop(self.slot as usize);
    }
}

impl Payload for Small {
    const KIND: &'static str = "smaller than a pointer";
    fn make(probe: &Probe, id: u8) -> Self {
        Small {
            slot: u16::try_from(probe.slot).unwrap(),
            id,
        }
    }
    fn id(&self) -> Option<u8> {
        Some(self.id)
    }
}

/// Zero sized, with Drop.
#[derive(Debug)]
struct Zst;

impl Drop for Zst {
    fn drop(&mut self) {
        count_drop(0);
    }
}

impl Payload for Zst {
    const KIND: &'static str = "zero sized";
    fn probe() -> Probe {
        let guard = ZST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        DROPS[0].store(0, SeqCst);
        Probe {
            slot: 0,
            _serial: Some(guard),
        }
    }
    fn make(_: &Probe, _: u8) -> Self {
        Zst
    }
    fn id(&self) -> Option<u8> {
        None
    }
    fn tag(_: u8) -> Option<u8> {
        None
    }
}

#[test]
fn timeout_race_payload_kinds_cover_every_kanal_ptr_layout() {
    let ptr = size_of::<*mut u8>();
    assert!(size_of::<Big>() > ptr);
    assert_eq!(size_of::<Boxed>(), ptr);
    assert!(size_of::<Small>() < ptr);
    assert_eq!(size_of::<Zst>(), 0);
}

fn channel<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
    match cap {
        Some(cap) => bounded(cap),
        None => unbounded(),
    }
}

const PLANS_OK: [Plan; 3] = [
    Plan::HeldAcrossDeadline,
    Plan::TakenAfterDeadline,
    Plan::CompletedAfterDeadline,
];

// ---------------------------------------------------------------------------
// recv_timeout
// ---------------------------------------------------------------------------

fn recv_timeout_ok<P: Payload>(cap: Option<usize>, plan: Plan) {
    let probe = P::probe();
    let (tx, rx) = channel::<P>(cap);
    let item = P::make(&probe, 7);
    let out = run_race(
        plan,
        || rx.recv_timeout(OP_TIMEOUT),
        || tx.send(item),
        || {},
    );
    let ctx = format!("{} cap {cap:?} {plan:?}", P::KIND);
    assert_eq!(out.peer, Ok(()), "{ctx}");
    let got = out
        .waiter
        .unwrap_or_else(|e| panic!("{ctx}: expected the message, got {e:?}"));
    assert_eq!(got.id(), P::tag(7), "{ctx}");
    assert_eq!(probe.drops(), 0, "{ctx}: delivered value dropped early");
    drop(got);
    assert_eq!(probe.drops(), 1, "{ctx}");
    assert!(
        matches!(rx.try_recv(), Ok(None)),
        "{ctx}: duplicate message"
    );
}

fn recv_timeout_timeout<P: Payload>(cap: Option<usize>) {
    let probe = P::probe();
    let (tx, rx) = channel::<P>(cap);
    let item = P::make(&probe, 3);
    let out = run_race(
        Plan::CancelledFirst,
        || rx.recv_timeout(OP_TIMEOUT),
        || tx.try_send(item),
        || {},
    );
    let ctx = format!("{} cap {cap:?}", P::KIND);
    assert!(
        matches!(out.waiter, Err(ReceiveErrorTimeout::Timeout)),
        "{ctx}: {:?}",
        out.waiter
    );
    // The cancelled waiter is gone: a later send queues (or, without a
    // buffer, fails and drops the value) instead of feeding a dead signal.
    let queued = cap != Some(0);
    assert_eq!(out.peer, Ok(queued), "{ctx}");
    if queued {
        assert_eq!(probe.drops(), 0, "{ctx}");
        let later = rx.try_recv().unwrap().expect("queued message lost");
        assert_eq!(later.id(), P::tag(3), "{ctx}");
    }
    drop(out);
    assert_eq!(probe.drops(), 1, "{ctx}");
}

fn recv_timeout_closed<P: Payload>(cap: Option<usize>) {
    let probe = P::probe();
    let (tx, rx) = channel::<P>(cap);
    let item = P::make(&probe, 4);
    let out = run_race(
        Plan::BeforePeer,
        || rx.recv_timeout(OP_TIMEOUT),
        || tx.send(item),
        || tx.close().unwrap(),
    );
    let ctx = format!("{} cap {cap:?}", P::KIND);
    assert!(
        matches!(out.waiter, Err(ReceiveErrorTimeout::Closed)),
        "{ctx}: {:?}",
        out.waiter
    );
    assert_eq!(out.peer, Err(SendError::Closed), "{ctx}");
    assert_eq!(
        probe.drops(),
        1,
        "{ctx}: rejected value must be dropped once"
    );
}

fn recv_timeout_matrix<P: Payload>() {
    for cap in [None, Some(0), Some(1)] {
        for plan in PLANS_OK {
            recv_timeout_ok::<P>(cap, plan);
        }
        recv_timeout_timeout::<P>(cap);
        recv_timeout_closed::<P>(cap);
    }
}

#[test]
fn timeout_race_recv_timeout_big() {
    recv_timeout_matrix::<Big>();
}

#[test]
fn timeout_race_recv_timeout_boxed() {
    recv_timeout_matrix::<Boxed>();
}

#[test]
fn timeout_race_recv_timeout_small() {
    recv_timeout_matrix::<Small>();
}

#[test]
fn timeout_race_recv_timeout_zst() {
    recv_timeout_matrix::<Zst>();
}

// ---------------------------------------------------------------------------
// send_timeout / send_option_timeout (rendezvous: only there can a receiver
// take a parked sender's signal outside the lock)
// ---------------------------------------------------------------------------

fn send_timeout_ok<P: Payload>(plan: Plan) {
    let probe = P::probe();
    let (tx, rx) = bounded::<P>(0);
    let item = P::make(&probe, 9);
    let out = run_race(
        plan,
        || tx.send_timeout(item, OP_TIMEOUT),
        || rx.try_recv(),
        || {},
    );
    let ctx = format!("{} {plan:?}", P::KIND);
    assert_eq!(out.waiter, Ok(()), "{ctx}");
    let got = out.peer.unwrap().expect("the peer took the signal");
    assert_eq!(got.id(), P::tag(9), "{ctx}");
    assert_eq!(
        probe.drops(),
        0,
        "{ctx}: sender dropped the delivered value"
    );
    drop(got);
    assert_eq!(probe.drops(), 1, "{ctx}");
}

fn send_timeout_failures<P: Payload>() {
    // Timeout: the sender drops the value it could not deliver, once.
    let probe = P::probe();
    let (tx, rx) = bounded::<P>(0);
    let item = P::make(&probe, 1);
    let out = run_race(
        Plan::CancelledFirst,
        || tx.send_timeout(item, OP_TIMEOUT),
        || rx.try_recv(),
        || {},
    );
    assert_eq!(out.waiter, Err(SendErrorTimeout::Timeout), "{}", P::KIND);
    assert!(
        matches!(out.peer, Ok(None)),
        "{}: cancelled send still visible",
        P::KIND
    );
    assert_eq!(
        probe.drops(),
        1,
        "{}: Timeout must drop the value once",
        P::KIND
    );
    drop(probe);

    // Closed before a receiver took it: dropped once, by the sender.
    let probe = P::probe();
    let (tx, rx) = bounded::<P>(0);
    let item = P::make(&probe, 2);
    let out = run_race(
        Plan::BeforePeer,
        || tx.send_timeout(item, OP_TIMEOUT),
        || rx.try_recv(),
        || rx.close().unwrap(),
    );
    assert_eq!(out.waiter, Err(SendErrorTimeout::Closed), "{}", P::KIND);
    assert!(matches!(out.peer, Err(ReceiveError::Closed)), "{}", P::KIND);
    assert_eq!(
        probe.drops(),
        1,
        "{}: Closed must drop the value once",
        P::KIND
    );
}

fn send_option_timeout_ok<P: Payload>(plan: Plan) {
    let probe = P::probe();
    let (tx, rx) = bounded::<P>(0);
    let item = P::make(&probe, 5);
    let out = run_race(
        plan,
        || {
            let mut data = Some(item);
            let r = tx.send_option_timeout(&mut data, OP_TIMEOUT);
            (r, data)
        },
        || rx.try_recv(),
        || {},
    );
    let ctx = format!("{} {plan:?}", P::KIND);
    let (sent, data) = out.waiter;
    assert_eq!(sent, Ok(()), "{ctx}");
    assert!(data.is_none(), "{ctx}: delivered value still in the option");
    let got = out.peer.unwrap().expect("the peer took the signal");
    assert_eq!(got.id(), P::tag(5), "{ctx}");
    assert_eq!(probe.drops(), 0, "{ctx}");
    drop(got);
    assert_eq!(probe.drops(), 1, "{ctx}");
}

fn send_option_timeout_failures<P: Payload>() {
    for (plan, expected) in [
        (Plan::CancelledFirst, SendErrorTimeout::Timeout),
        (Plan::BeforePeer, SendErrorTimeout::Closed),
    ] {
        let probe = P::probe();
        let (tx, rx) = bounded::<P>(0);
        let item = P::make(&probe, 6);
        let close = plan == Plan::BeforePeer;
        let out = run_race(
            plan,
            || {
                let mut data = Some(item);
                let r = tx.send_option_timeout(&mut data, OP_TIMEOUT);
                (r, data)
            },
            || rx.try_recv(),
            || {
                if close {
                    rx.close().unwrap();
                }
            },
        );
        let ctx = format!("{} {plan:?}", P::KIND);
        let (sent, data) = out.waiter;
        assert_eq!(sent, Err(expected), "{ctx}");
        let data = data.unwrap_or_else(|| panic!("{ctx}: value not handed back"));
        assert_eq!(data.id(), P::tag(6), "{ctx}");
        assert_eq!(probe.drops(), 0, "{ctx}: handed-back value was dropped");
        drop(data);
        assert_eq!(probe.drops(), 1, "{ctx}");
    }
}

fn send_matrix<P: Payload>() {
    for plan in PLANS_OK {
        send_timeout_ok::<P>(plan);
        send_option_timeout_ok::<P>(plan);
    }
    send_timeout_failures::<P>();
    send_option_timeout_failures::<P>();
}

#[test]
fn timeout_race_send_timeout_big() {
    send_matrix::<Big>();
}

#[test]
fn timeout_race_send_timeout_boxed() {
    send_matrix::<Boxed>();
}

#[test]
fn timeout_race_send_timeout_small() {
    send_matrix::<Small>();
}

#[test]
fn timeout_race_send_timeout_zst() {
    send_matrix::<Zst>();
}

#[test]
fn timeout_race_buffered_send_timeout_times_out_and_drops_once() {
    // With a buffer, a receiver moves a parked sender's value into the queue
    // under the lock, so only plain timeouts remain.
    let probe = Big::probe();
    let (tx, rx) = bounded::<Big>(1);
    tx.send(Big::make(&probe, 0)).unwrap();
    assert_eq!(
        tx.send_timeout(Big::make(&probe, 1), Duration::ZERO),
        Err(SendErrorTimeout::Timeout)
    );
    assert_eq!(probe.drops(), 1);
    drop(rx);
    drop(tx);
    assert_eq!(probe.drops(), 2);
}

// ---------------------------------------------------------------------------
// close() and dropped clones while a handoff is held at the gate
// ---------------------------------------------------------------------------

#[test]
fn timeout_race_close_while_handoff_held_still_delivers_to_recv_timeout() {
    for plan in [Plan::HeldAcrossDeadline, Plan::TakenAfterDeadline] {
        let probe = Big::probe();
        let (tx, rx) = unbounded::<Big>();
        let item = Big::make(&probe, 11);
        let out = run_race(
            plan,
            || rx.recv_timeout(OP_TIMEOUT),
            || tx.send(item),
            || rx.close().unwrap(),
        );
        assert_eq!(out.peer, Ok(()), "{plan:?}");
        let got = out.waiter.unwrap_or_else(|e| panic!("{plan:?}: {e:?}"));
        assert_eq!(got.id, 11);
        drop(got);
        assert_eq!(probe.drops(), 1, "{plan:?}");
        assert!(rx.is_closed());
    }
}

#[test]
fn timeout_race_dropping_spare_senders_while_handoff_held_still_delivers() {
    let probe = Big::probe();
    let (tx, rx) = unbounded::<Big>();
    let spares: Vec<_> = (0..3).map(|_| tx.clone()).collect();
    let item = Big::make(&probe, 12);
    let out = run_race(
        Plan::HeldAcrossDeadline,
        || rx.recv_timeout(OP_TIMEOUT),
        || tx.send(item),
        move || drop(spares),
    );
    assert_eq!(out.peer, Ok(()));
    let got = out.waiter.unwrap();
    assert_eq!(got.id, 12);
    drop(got);
    assert_eq!(probe.drops(), 1);
    assert_eq!(rx.sender_count(), 1);
}

#[test]
fn timeout_race_close_while_handoff_held_still_delivers_send_timeout() {
    for plan in [Plan::HeldAcrossDeadline, Plan::TakenAfterDeadline] {
        let probe = Big::probe();
        let (tx, rx) = bounded::<Big>(0);
        let item = Big::make(&probe, 13);
        let out = run_race(
            plan,
            || tx.send_timeout(item, OP_TIMEOUT),
            || rx.try_recv(),
            || tx.close().unwrap(),
        );
        assert_eq!(out.waiter, Ok(()), "{plan:?}");
        let got = out.peer.unwrap().unwrap();
        assert_eq!(got.id, 13);
        drop(got);
        assert_eq!(probe.drops(), 1, "{plan:?}");
    }
}

#[test]
fn timeout_race_close_while_handoff_held_still_delivers_send_option_timeout() {
    for plan in [Plan::HeldAcrossDeadline, Plan::TakenAfterDeadline] {
        let probe = Big::probe();
        let (tx, rx) = bounded::<Big>(0);
        let spare_rx = rx.clone();
        let item = Big::make(&probe, 14);
        let out = run_race(
            plan,
            || {
                let mut data = Some(item);
                (tx.send_option_timeout(&mut data, OP_TIMEOUT), data)
            },
            || rx.try_recv(),
            || {
                drop(spare_rx);
                tx.close().unwrap();
            },
        );
        let (sent, data) = out.waiter;
        assert_eq!(sent, Ok(()), "{plan:?}");
        assert!(data.is_none(), "{plan:?}");
        let got = out.peer.unwrap().unwrap();
        assert_eq!(got.id, 14);
        drop(got);
        assert_eq!(probe.drops(), 1, "{plan:?}");
    }
}

#[test]
fn timeout_race_dropping_all_senders_before_handoff_reports_closed() {
    let probe = Big::probe();
    let (tx, rx) = unbounded::<Big>();
    let out = run_race(
        Plan::BeforePeer,
        || rx.recv_timeout(OP_TIMEOUT),
        || (),
        move || drop(tx),
    );
    assert!(matches!(out.waiter, Err(ReceiveErrorTimeout::Closed)));
    assert_eq!(probe.drops(), 0);
}

#[test]
fn timeout_race_dropping_all_receivers_before_handoff_reports_closed() {
    let probe = Big::probe();
    let (tx, rx) = bounded::<Big>(0);
    let item = Big::make(&probe, 15);
    let out = run_race(
        Plan::BeforePeer,
        || {
            let mut data = Some(item);
            (tx.send_option_timeout(&mut data, OP_TIMEOUT), data)
        },
        || (),
        move || drop(rx),
    );
    let (sent, data) = out.waiter;
    assert_eq!(sent, Err(SendErrorTimeout::Closed));
    assert_eq!(data.map(|d| d.id), Some(15), "value must be handed back");
    assert_eq!(probe.drops(), 1);
}

// ---------------------------------------------------------------------------
// Every peer operation that can take a waiter's signal outside the lock
// ---------------------------------------------------------------------------

#[test]
fn timeout_race_every_sending_peer_completes_a_held_recv_timeout() {
    type Peer = fn(&Sender<Big>, Big) -> bool;
    #[allow(unused_mut)]
    let mut peers: Vec<(&str, Peer)> = vec![
        ("send", |tx, v| tx.send(v).is_ok()),
        ("send_timeout", |tx, v| {
            tx.send_timeout(v, OP_TIMEOUT).is_ok()
        }),
        ("send_option_timeout", |tx, v| {
            let mut d = Some(v);
            tx.send_option_timeout(&mut d, OP_TIMEOUT).is_ok() && d.is_none()
        }),
        ("try_send", |tx, v| tx.try_send(v) == Ok(true)),
        ("try_send_option", |tx, v| {
            let mut d = Some(v);
            tx.try_send_option(&mut d) == Ok(true) && d.is_none()
        }),
        ("try_send_realtime", |tx, v| {
            tx.try_send_realtime(v) == Ok(true)
        }),
        ("try_send_option_realtime", |tx, v| {
            let mut d = Some(v);
            tx.try_send_option_realtime(&mut d) == Ok(true) && d.is_none()
        }),
    ];
    #[cfg(feature = "async")]
    {
        let send_future: Peer = |tx, v| futures::executor::block_on(tx.as_async().send(v)).is_ok();
        peers.push(("AsyncSender::send", send_future));
    }
    for (name, peer) in peers {
        for cap in [None, Some(0)] {
            let probe = Big::probe();
            let (tx, rx) = channel::<Big>(cap);
            let item = Big::make(&probe, 21);
            let out = run_race(
                Plan::HeldAcrossDeadline,
                || rx.recv_timeout(OP_TIMEOUT),
                || peer(&tx, item),
                || {},
            );
            assert!(out.peer, "{name} cap {cap:?}: peer failed");
            let got = out
                .waiter
                .unwrap_or_else(|e| panic!("{name} cap {cap:?}: {e:?}"));
            assert_eq!(got.id, 21, "{name}");
            drop(got);
            assert_eq!(probe.drops(), 1, "{name} cap {cap:?}");
        }
    }
}

#[test]
fn timeout_race_every_receiving_peer_completes_a_held_send_timeout() {
    type Peer = fn(&Receiver<Big>) -> Option<Big>;
    #[allow(unused_mut)]
    let mut peers: Vec<(&str, Peer)> = vec![
        ("recv", |rx| rx.recv().ok()),
        ("recv_timeout", |rx| rx.recv_timeout(OP_TIMEOUT).ok()),
        ("try_recv", |rx| rx.try_recv().ok().flatten()),
        ("try_recv_realtime", |rx| {
            rx.try_recv_realtime().ok().flatten()
        }),
    ];
    #[cfg(feature = "async")]
    {
        let recv_future: Peer = |rx| futures::executor::block_on(rx.as_async().recv()).ok();
        peers.push(("AsyncReceiver::recv", recv_future));
    }
    for (name, peer) in peers {
        for plan in [Plan::HeldAcrossDeadline, Plan::TakenAfterDeadline] {
            // send_timeout
            let probe = Big::probe();
            let (tx, rx) = bounded::<Big>(0);
            let item = Big::make(&probe, 22);
            let out = run_race(
                plan,
                || tx.send_timeout(item, OP_TIMEOUT),
                || peer(&rx),
                || {},
            );
            assert_eq!(out.waiter, Ok(()), "{name} {plan:?}");
            let got = out
                .peer
                .unwrap_or_else(|| panic!("{name}: nothing received"));
            assert_eq!(got.id, 22, "{name}");
            drop(got);
            assert_eq!(probe.drops(), 1, "{name} {plan:?}");

            // send_option_timeout
            let probe = Big::probe();
            let (tx, rx) = bounded::<Big>(0);
            let item = Big::make(&probe, 23);
            let out = run_race(
                plan,
                || {
                    let mut d = Some(item);
                    (tx.send_option_timeout(&mut d, OP_TIMEOUT), d)
                },
                || peer(&rx),
                || {},
            );
            assert_eq!(out.waiter.0, Ok(()), "{name} {plan:?}");
            assert!(out.waiter.1.is_none(), "{name} {plan:?}");
            let got = out
                .peer
                .unwrap_or_else(|| panic!("{name}: nothing received"));
            assert_eq!(got.id, 23, "{name}");
            drop(got);
            assert_eq!(probe.drops(), 1, "{name} {plan:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// Panicking Drop after a timeout cancellation
// ---------------------------------------------------------------------------

/// Payload whose drop panics when armed.
#[derive(Debug)]
struct Bomb {
    slot: u32,
    armed: bool,
    _pad: [u64; 2],
}

impl Bomb {
    fn new(probe: &Probe, armed: bool) -> Self {
        Bomb {
            slot: probe.slot as u32,
            armed,
            _pad: [0; 2],
        }
    }
}

impl Drop for Bomb {
    fn drop(&mut self) {
        count_drop(self.slot as usize);
        if self.armed {
            panic!("payload Drop panicked (expected by this test)");
        }
    }
}

#[test]
fn timeout_race_panicking_drop_after_send_timeout_cancel_releases_the_lock() {
    let probe = Probe::new();
    let (tx, rx) = bounded::<Bomb>(1);
    tx.send(Bomb::new(&probe, false)).unwrap();
    // Full buffer: the send parks, its deadline has already passed, the
    // cancel succeeds and the value is dropped -- and that drop panics.
    let r = catch_unwind(AssertUnwindSafe(|| {
        tx.send_timeout(Bomb::new(&probe, true), Duration::ZERO)
    }));
    assert!(r.is_err(), "Timeout must drop the value: {r:?}");
    assert_eq!(probe.drops(), 1);
    // The non-blocking probes would report nothing if the lock were held.
    let filler = rx.try_recv_realtime().unwrap();
    assert!(filler.is_some(), "channel lock still held after the panic");
    drop(filler);
    assert_eq!(tx.try_send_realtime(Bomb::new(&probe, false)), Ok(true));
    assert!(rx.recv_timeout(OP_TIMEOUT).is_ok());
    assert_eq!(probe.drops(), 3);
}

#[test]
fn timeout_race_panicking_drop_after_send_timeout_closed_releases_the_lock() {
    let probe = Probe::new();
    let (tx, rx) = bounded::<Bomb>(0);
    let item = Bomb::new(&probe, true);
    let out = run_race(
        Plan::BeforePeer,
        || catch_unwind(AssertUnwindSafe(|| tx.send_timeout(item, OP_TIMEOUT))).is_err(),
        || (),
        || rx.close().unwrap(),
    );
    assert!(
        out.waiter,
        "Closed must drop the value (and the drop panics)"
    );
    assert_eq!(probe.drops(), 1);
    // try_recv_realtime returns Ok(None) instead of Closed if it could not
    // take the lock.
    assert!(matches!(rx.try_recv_realtime(), Err(ReceiveError::Closed)));
}

#[cfg(feature = "async")]
#[test]
fn timeout_race_panicking_drop_of_cancelled_send_future_releases_the_lock() {
    use async_support::*;
    let probe = Probe::new();
    let (tx, rx) = bounded_async::<Bomb>(1);
    tx.try_send(Bomb::new(&probe, false)).unwrap();
    let mut fut = Box::pin(tx.send(Bomb::new(&probe, true)));
    let (waker, _) = counting_waker();
    assert!(poll_once(fut.as_mut(), &waker).is_pending());
    let r = catch_unwind(AssertUnwindSafe(move || drop(fut)));
    assert!(r.is_err(), "the cancelled future must drop its value");
    assert_eq!(probe.drops(), 1);
    let filler = rx.try_recv_realtime().unwrap();
    assert!(filler.is_some(), "channel lock still held after the panic");
    drop(filler);
    assert_eq!(probe.drops(), 2);
}

// ---------------------------------------------------------------------------
// Signal::wait_after_timeout
// ---------------------------------------------------------------------------

mod signal_level {
    use super::*;

    /// A receive signal that already timed out: state `LOCKED_STARVATION`
    /// with the current thread registered, as `recv_timeout` leaves it.
    fn timed_out_signal(slot: &mut MaybeUninit<u64>) -> Signal<u64> {
        let sig = Signal::new_sync(KanalPtr::new_write_address_ptr(slot.as_mut_ptr()));
        assert!(!sig.wait_timeout(Instant::now()));
        sig
    }

    fn count_parks(
        mut on_park: impl FnMut(usize) + 'static,
    ) -> (test_hooks::HookGuard, Rc<Cell<usize>>) {
        let parks = Rc::new(Cell::new(0));
        let p = parks.clone();
        let guard = test_hooks::set(move |point| {
            if point == Hook::AwaitPeerPark {
                p.set(p.get() + 1);
                on_park(p.get());
            }
            None
        });
        (guard, parks)
    }

    #[test]
    fn timeout_race_wait_after_timeout_ignores_spurious_unparks() {
        const SPURIOUS: usize = 50;
        let mut slot = MaybeUninit::uninit();
        let sig = timed_out_signal(&mut slot);
        let term = RefCell::new(Some(sig.get_terminator()));
        let (_g, parks) = count_parks(move |n| {
            if n <= SPURIOUS {
                // Wake ourselves without completing the signal.
                thread::current().unpark();
            } else if let Some(t) = term.borrow_mut().take() {
                // Completion after the state check, before park().
                unsafe { t.send(42) }
            }
        });
        assert!(sig.wait_after_timeout());
        assert_eq!(parks.get(), SPURIOUS + 1, "returned before completion");
        assert_eq!(unsafe { sig.assume_init() }, 42);
    }

    #[test]
    fn timeout_race_wait_after_timeout_sees_completion_from_another_thread_before_park() {
        let mut slot = MaybeUninit::uninit();
        let sig = timed_out_signal(&mut slot);
        let term = sig.get_terminator();
        let (go, done) = (Flag::default(), Flag::default());
        thread::scope(|s| {
            let (go2, done2) = (go.clone(), done.clone());
            let peer = s.spawn(move || {
                go2.expect("the waiter to reach park");
                unsafe { term.send(7) };
                done2.raise();
            });
            let (_g, parks) = count_parks(move |n| {
                if n == 1 {
                    go.raise();
                    done.expect("the peer to complete the signal");
                }
            });
            assert!(sig.wait_after_timeout());
            assert_eq!(parks.get(), 1);
            join(peer);
        });
        assert_eq!(unsafe { sig.assume_init() }, 7);
    }

    #[test]
    fn timeout_race_wait_after_timeout_survives_concurrent_spurious_unparks() {
        let mut slot = MaybeUninit::uninit();
        let sig = timed_out_signal(&mut slot);
        let term = sig.get_terminator();
        let me = thread::current();
        thread::scope(|s| {
            s.spawn(move || {
                for _ in 0..if cfg!(miri) { 20 } else { 2000 } {
                    me.unpark();
                    thread::yield_now();
                }
                unsafe { term.send(9) };
            });
            assert!(sig.wait_after_timeout());
        });
        assert_eq!(unsafe { sig.assume_init() }, 9);
    }

    #[test]
    fn timeout_race_wait_after_timeout_reports_termination() {
        let mut slot = MaybeUninit::uninit();
        let sig = timed_out_signal(&mut slot);
        let term = RefCell::new(Some(sig.get_terminator()));
        let (_g, parks) = count_parks(move |_| {
            if let Some(t) = term.borrow_mut().take() {
                unsafe { t.terminate() }
            }
        });
        assert!(!sig.wait_after_timeout());
        assert_eq!(parks.get(), 1);
        assert!(sig.is_terminated());
    }

    #[test]
    fn timeout_race_wait_after_timeout_returns_at_once_when_already_completed() {
        let mut slot = MaybeUninit::uninit();
        let sig = timed_out_signal(&mut slot);
        unsafe { sig.get_terminator().send(5) };
        let (_g, parks) = count_parks(|_| {});
        assert!(sig.wait_after_timeout());
        assert_eq!(parks.get(), 0);
        assert_eq!(unsafe { sig.assume_init() }, 5);
    }

    #[test]
    fn timeout_race_wait_after_timeout_on_untouched_signal_waits_for_peer() {
        // State still LOCKED (no timed-out wait before): delegates to wait().
        let mut slot = MaybeUninit::<[u64; 4]>::uninit();
        let sig = Signal::new_sync(KanalPtr::new_write_address_ptr(slot.as_mut_ptr()));
        let term = sig.get_terminator();
        thread::scope(|s| {
            s.spawn(move || unsafe { term.send([1, 2, 3, 4]) });
            assert!(sig.wait_after_timeout());
        });
        assert_eq!(unsafe { slot.assume_init() }, [1, 2, 3, 4]);
    }
}

// ---------------------------------------------------------------------------
// Async peers and futures dropped after their signal was taken
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
mod async_support {
    use super::*;
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};

    pub(super) struct WakeCount(pub(super) AtomicUsize);

    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, SeqCst);
        }
    }

    pub(super) fn counting_waker() -> (Waker, Arc<WakeCount>) {
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        (futures::task::waker(count.clone()), count)
    }

    pub(super) fn poll_once<F: Future>(fut: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        fut.poll(&mut Context::from_waker(waker))
    }
}

#[cfg(feature = "async")]
mod async_peers {
    use super::async_support::*;
    use super::*;

    fn async_sender_completes_recv_timeout<P: Payload>() {
        for plan in PLANS_OK {
            let probe = P::probe();
            let (tx, rx) = unbounded_async::<P>();
            let item = P::make(&probe, 31);
            let out = run_race(
                plan,
                || rx.as_sync().recv_timeout(OP_TIMEOUT),
                || futures::executor::block_on(tx.send(item)),
                || {},
            );
            let ctx = format!("{} {plan:?}", P::KIND);
            assert_eq!(out.peer, Ok(()), "{ctx}");
            let got = out.waiter.unwrap_or_else(|e| panic!("{ctx}: {e:?}"));
            assert_eq!(got.id(), P::tag(31), "{ctx}");
            drop(got);
            assert_eq!(probe.drops(), 1, "{ctx}");
        }
    }

    fn async_receiver_completes_send_timeout<P: Payload>() {
        for plan in PLANS_OK {
            let probe = P::probe();
            let (tx, rx) = bounded_async::<P>(0);
            let item = P::make(&probe, 32);
            let out = run_race(
                plan,
                || tx.as_sync().send_timeout(item, OP_TIMEOUT),
                || futures::executor::block_on(rx.recv()),
                || {},
            );
            let ctx = format!("{} {plan:?}", P::KIND);
            assert_eq!(out.waiter, Ok(()), "{ctx}");
            let got = out.peer.unwrap_or_else(|e| panic!("{ctx}: {e:?}"));
            assert_eq!(got.id(), P::tag(32), "{ctx}");
            drop(got);
            assert_eq!(probe.drops(), 1, "{ctx}");
        }
    }

    /// A queued send future is dropped while a receiver that took its signal
    /// is held: the drop must wait for the receiver, which gets the value.
    fn dropped_send_future_waits_for_taken_signal<P: Payload>() {
        let probe = P::probe();
        let (tx, rx) = bounded_async::<P>(0);
        let mut fut = Box::pin(tx.send(P::make(&probe, 33)));
        let (waker, wakes) = counting_waker();
        assert!(poll_once(fut.as_mut(), &waker).is_pending());
        let script = Script::default();
        let got = thread::scope(|s| {
            let _open = OpenOnPanic(&script);
            let peer_script = script.clone();
            let rx = &rx;
            let peer = s.spawn(move || {
                let _hook = test_hooks::set(peer_script.peer_hook(Plan::HeldAcrossDeadline));
                rx.as_sync().recv()
            });
            script
                .acquired
                .expect("the receiver to take the send future's signal");
            {
                let _hook = test_hooks::set(script.waiter_hook(Plan::HeldAcrossDeadline));
                drop(fut);
            }
            script.release.raise();
            join(peer)
        });
        assert!(
            !script.gate_expired.is_raised(),
            "{}: gate timed out",
            P::KIND
        );
        assert!(
            script.waited.is_raised(),
            "{}: drop returned while the receiver still owned the signal",
            P::KIND
        );
        let got = got.unwrap_or_else(|e| panic!("{}: {e:?}", P::KIND));
        assert_eq!(got.id(), P::tag(33), "{}", P::KIND);
        assert_eq!(probe.drops(), 0, "{}: transferred value dropped", P::KIND);
        drop(got);
        assert_eq!(probe.drops(), 1, "{}", P::KIND);
        assert_eq!(wakes.0.load(SeqCst), 1, "{}", P::KIND);
    }

    /// A queued receive future is dropped while a sender that took its
    /// signal is held: the drop must wait, then drop the value exactly once.
    fn dropped_receive_future_waits_and_drops_value_once<P: Payload>() {
        let probe = P::probe();
        let (tx, rx) = bounded_async::<P>(0);
        let mut fut = Box::pin(rx.recv());
        let (waker, _) = counting_waker();
        assert!(poll_once(fut.as_mut(), &waker).is_pending());
        let script = Script::default();
        let item = P::make(&probe, 34);
        let sent = thread::scope(|s| {
            let _open = OpenOnPanic(&script);
            let peer_script = script.clone();
            let tx = &tx;
            let peer = s.spawn(move || {
                let _hook = test_hooks::set(peer_script.peer_hook(Plan::HeldAcrossDeadline));
                tx.as_sync().send(item)
            });
            script
                .acquired
                .expect("the sender to take the receive future's signal");
            {
                let _hook = test_hooks::set(script.waiter_hook(Plan::HeldAcrossDeadline));
                drop(fut);
            }
            assert_eq!(
                probe.drops(),
                1,
                "{}: received value not dropped once",
                P::KIND
            );
            script.release.raise();
            join(peer)
        });
        assert!(
            !script.gate_expired.is_raised(),
            "{}: gate timed out",
            P::KIND
        );
        assert!(script.waited.is_raised(), "{}: drop did not wait", P::KIND);
        assert_eq!(sent, Ok(()), "{}", P::KIND);
        assert_eq!(probe.drops(), 1, "{}", P::KIND);
    }

    fn cancelled_futures<P: Payload>() {
        // Not taken yet: the send future drops its value, the receive future
        // just leaves the wait list.
        let probe = P::probe();
        let (tx, rx) = bounded_async::<P>(0);
        let mut fut = Box::pin(tx.send(P::make(&probe, 35)));
        let (waker, _) = counting_waker();
        assert!(poll_once(fut.as_mut(), &waker).is_pending());
        drop(fut);
        assert_eq!(probe.drops(), 1, "{}", P::KIND);
        assert!(matches!(rx.try_recv(), Ok(None)), "{}", P::KIND);

        let mut fut = Box::pin(rx.recv());
        assert!(poll_once(fut.as_mut(), &waker).is_pending());
        drop(fut);
        let mut d = Some(P::make(&probe, 36));
        assert_eq!(tx.try_send_option(&mut d), Ok(false), "{}", P::KIND);
        drop(d);
        assert_eq!(probe.drops(), 2, "{}", P::KIND);
    }

    fn matrix<P: Payload>() {
        async_sender_completes_recv_timeout::<P>();
        async_receiver_completes_send_timeout::<P>();
        dropped_send_future_waits_for_taken_signal::<P>();
        dropped_receive_future_waits_and_drops_value_once::<P>();
        cancelled_futures::<P>();
    }

    #[test]
    fn timeout_race_async_big() {
        matrix::<Big>();
    }

    #[test]
    fn timeout_race_async_boxed() {
        matrix::<Boxed>();
    }

    #[test]
    fn timeout_race_async_small() {
        matrix::<Small>();
    }

    #[test]
    fn timeout_race_async_zst() {
        matrix::<Zst>();
    }
}

// ---------------------------------------------------------------------------
// Plain timeouts and stress
// ---------------------------------------------------------------------------

#[test]
fn timeout_race_plain_timeouts_without_peers() {
    let (_tx, rx) = unbounded::<u64>();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(5)),
        Err(ReceiveErrorTimeout::Timeout)
    );
    let probe = Big::probe();
    let (tx, _rx) = bounded::<Big>(0);
    assert_eq!(
        tx.send_timeout(Big::make(&probe, 1), Duration::from_millis(5)),
        Err(SendErrorTimeout::Timeout)
    );
    assert_eq!(probe.drops(), 1);
    let mut d = Some(Big::make(&probe, 2));
    assert_eq!(
        tx.send_option_timeout(&mut d, Duration::from_millis(5)),
        Err(SendErrorTimeout::Timeout)
    );
    assert_eq!(d.as_ref().map(|d| d.id), Some(2));
    drop(d);
    assert_eq!(probe.drops(), 2);
}

/// Not a deterministic test: senders randomly stall after taking a signal
/// while receivers use short timeouts. Skipped under Miri.
#[test]
#[cfg_attr(miri, ignore)]
fn timeout_race_stress_many_stalled_handoffs_lose_and_duplicate_nothing() {
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 200;
    let (tx, rx) = unbounded::<u64>();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let consumers: Vec<_> = (0..4)
        .map(|_| {
            let rx = rx.clone();
            let seen = seen.clone();
            thread::spawn(move || loop {
                match rx.recv_timeout(Duration::from_micros(300)) {
                    Ok(v) => seen.lock().unwrap().push(v),
                    Err(ReceiveErrorTimeout::Timeout) => {}
                    Err(e) => {
                        assert!(rx.is_disconnected(), "spurious {e:?} on a live channel");
                        break;
                    }
                }
            })
        })
        .collect();
    drop(rx);
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                let stall = Rc::new(Cell::new(false));
                let s = stall.clone();
                let _hook = test_hooks::set(move |point| {
                    if point == Hook::Handoff && s.get() {
                        thread::sleep(Duration::from_micros(400));
                    }
                    None
                });
                for i in 0..PER_PRODUCER {
                    stall.set(i % 3 == 0);
                    thread::sleep(Duration::from_micros(150));
                    tx.send(p * PER_PRODUCER + i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    for p in producers {
        p.join().unwrap();
    }
    for c in consumers {
        c.join().unwrap();
    }
    let mut seen = seen.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, (0..PRODUCERS * PER_PRODUCER).collect::<Vec<_>>());
}
