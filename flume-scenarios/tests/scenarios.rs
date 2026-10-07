//! Small, deterministic-ish versions of the scenarios, sized to run under Miri:
//! `MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" cargo +nightly miri test -- --test-threads=1`
//! (Counters are leaked on purpose so payloads can hold `&'static`.)

use flume_scenarios::common::Outcome;
use flume_scenarios::{repro, s1, s2, s3, s4, s5};
use std::time::Duration;

fn big(n: u64, miri: u64) -> u64 {
    if cfg!(miri) {
        miri
    } else {
        n
    }
}

fn assert_ok(o: Outcome) {
    o.print();
    assert!(o.ok, "{}: {}", o.name, o.detail);
}

#[test]
fn s1_deadline_edge() {
    assert_ok(s1::run::<0>(if cfg!(miri) { 0.6 } else { 3.0 }, 1, false));
}

#[test]
fn s2_mixed_mpmc() {
    assert_ok(s2::run::<0>(big(20_000, 40), None, false));
    assert_ok(s2::run::<476>(big(5_000, 20), Some(4), false));
}

#[test]
fn s3_recv_states() {
    assert_ok(s3::states_recv());
}

#[test]
fn s3_send_states() {
    assert_ok(s3::states_send());
}

#[test]
fn s3_recv_cancel_stress() {
    assert_ok(s3::recv_stress::<0>(big(20_000, 30), false));
}

#[test]
fn s3_send_cancel_stress() {
    assert_ok(s3::send_stress::<0>(big(5_000, 20), false));
}

#[test]
fn s4_disconnect() {
    // BUG-1 can make a parked recv_timeout miss the disconnect (reported, not asserted);
    // exactly-once and no-leak are asserted.
    let o = s4::last_sender::<0>(big(50, 3), false);
    o.print();
    assert!(o.detail.contains("iters-with-loss/dup=0") && o.detail.contains("live_after=0"), "{}", o.detail);
    assert_ok(s4::last_receiver::<0>(big(50, 3), false));
    assert_ok(s4::weak_sender(big(50, 2)));
}

#[test]
fn s5_panicking_payload_drop() {
    assert_ok(s5::payload_drop());
}

#[test]
fn s5_panicking_waker() {
    let o = s5::panicking_waker();
    o.print();
}

/// Known defects: these assert the *buggy* behaviour so a fixed flume would flip them.
#[test]
fn bug1_orphaned_sync_waiter() {
    let o = repro::orphaned_recv_timeout(Duration::from_millis(600), Duration::from_millis(150));
    o.print();
    assert!(o.status.contains("REPRODUCED"), "{}", o.detail);
}

#[test]
fn bug2_sticky_woken() {
    let o = repro::sticky_woken(big(1000, 20));
    o.print();
    assert!(o.status.contains("REPRODUCED"), "{}", o.detail);
}
