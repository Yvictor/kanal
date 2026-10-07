//! Scenario 1: recv_timeout(100ms) consumers racing producers at the deadline edge.
//!
//! Each channel has two OS-thread consumers looping `recv_timeout(100ms)`; before
//! every call a consumer publishes the instant it armed. A producer thread reads
//! that instant and sends a burst of 1-3 messages at `armed + 100ms + U(-300us, +300us)`,
//! i.e. right around the moment the consumer times out and removes its hook.
//! Oracles: exactly-once, payload integrity, no leaks, and send->recv latency
//! (a lost wakeup shows up as a ~100 ms latency).

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::sync::Arc;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_millis(100);

pub fn run<const P: usize>(secs: f64, chans: usize, contended: bool) -> Outcome {
    let name = format!(
        "S1 deadline-edge {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs_f64(secs + 60.0));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let cap = (chans as f64 * (secs * 30.0 + 50.0) * 3.0) as usize + 1000;
    let seen = Arc::new(Seen::new(cap));
    let next_id = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Lat::default());
    let stats = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]); // ok, ok_after_deadline, timeouts, corrupt
    let end = now_ns() + (secs * 1e9) as u64;

    let mut hs = vec![];
    for c in 0..chans {
        let (tx, rx) = flume::unbounded::<Msg<P>>();
        let arms: Arc<[AtomicU64; 2]> = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
        for k in 0..2 {
            let rx = rx.clone();
            let arms = arms.clone();
            let (seen, lat, stats) = (seen.clone(), lat.clone(), stats.clone());
            hs.push(std::thread::spawn(move || loop {
                let t0 = now_ns();
                arms[k].store(t0, SeqCst);
                let r = rx.recv_timeout(TIMEOUT);
                let t1 = now_ns();
                match r {
                    Ok(m) => {
                        if !m.intact() {
                            stats[3].fetch_add(1, Relaxed);
                        }
                        seen.mark(m.id);
                        lat.record(t1.saturating_sub(m.ts));
                        stats[0].fetch_add(1, Relaxed);
                        if t1 - t0 >= TIMEOUT.as_nanos() as u64 {
                            stats[1].fetch_add(1, Relaxed);
                        }
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        stats[2].fetch_add(1, Relaxed);
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
            }));
        }
        drop(rx);
        let next_id = next_id.clone();
        hs.push(std::thread::spawn(move || {
            let mut rng = Rng::new(c as u64 + 7);
            let mut last = [0u64; 2];
            let mut k = 0;
            while now_ns() < end {
                k ^= 1;
                // wait for consumer k to arm a new recv_timeout
                let a = loop {
                    let a = arms[k].load(SeqCst);
                    if a != 0 && a != last[k] {
                        break a;
                    }
                    if now_ns() > end {
                        return;
                    }
                    std::thread::sleep(Duration::from_micros(200));
                };
                last[k] = a;
                let off = rng.below(600_000) as i64 - 300_000;
                let target = (a as i64 + TIMEOUT.as_nanos() as i64 + off) as u64;
                spin_until(target);
                for _ in 0..1 + rng.below(3) {
                    let id = next_id.fetch_add(1, Relaxed);
                    if id as usize >= cap {
                        return;
                    }
                    tx.send(Msg::<P>::new(id, ctr)).unwrap();
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let sent = next_id.load(SeqCst).min(cap as u64);
    let (miss, dup) = seen.summary(sent);
    let corrupt = stats[3].load(SeqCst);
    let leak = ctr.live();
    let ok = miss == 0 && dup == 0 && corrupt == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "sent={sent} missing={miss} dup={dup} corrupt={corrupt} live_after={leak} ok={} ok_at/after_deadline={} timeouts={} lat: {}",
            stats[0].load(SeqCst),
            stats[1].load(SeqCst),
            stats[2].load(SeqCst),
            lat.describe()
        ),
    )
}
