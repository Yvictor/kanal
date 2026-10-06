//! Stress the timeout paths against live peers: with timeouts short enough to
//! keep racing the deadline, no message may be lost, duplicated, dropped twice
//! or leaked, and a live channel must never report `Closed`.

use kanal::{bounded, unbounded, ReceiveErrorTimeout, SendErrorTimeout};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const RUN_FOR: Duration = Duration::from_secs(3);

/// Payload whose drops are counted.
struct Tracked {
    id: u64,
    drops: Arc<AtomicUsize>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn recv_timeout_mpmc_exactly_once() {
    for (producers, consumers, timeout_us) in [(4, 4, 50), (8, 2, 1), (2, 8, 200)] {
        let (tx, rx) = unbounded::<Tracked>();
        let drops = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let consumer_handles: Vec<_> = (0..consumers)
            .map(|_| {
                let rx = rx.clone();
                let received = received.clone();
                thread::spawn(move || {
                    let mut local = Vec::new();
                    loop {
                        match rx.recv_timeout(Duration::from_micros(timeout_us)) {
                            Ok(item) => local.push(item.id),
                            Err(ReceiveErrorTimeout::Timeout) => {}
                            Err(e) => {
                                assert!(rx.is_disconnected(), "spurious {e:?} on a live channel");
                                break;
                            }
                        }
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();
        drop(rx);
        let sent = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + RUN_FOR;
        let producer_handles: Vec<_> = (0..producers)
            .map(|p| {
                let tx = tx.clone();
                let drops = drops.clone();
                let sent = sent.clone();
                thread::spawn(move || {
                    let mut i = 0u64;
                    while Instant::now() < deadline {
                        tx.send(Tracked {
                            id: (p as u64) << 32 | i,
                            drops: drops.clone(),
                        })
                        .unwrap();
                        sent.fetch_add(1, Ordering::SeqCst);
                        i += 1;
                        if i % 64 == 0 {
                            // Vary the arrival pattern so sends keep hitting
                            // parked receivers near their deadline.
                            thread::sleep(Duration::from_micros(i % 7 * 20));
                        }
                    }
                })
            })
            .collect();
        drop(tx);
        for h in producer_handles {
            h.join().unwrap();
        }
        for h in consumer_handles {
            h.join().unwrap();
        }
        let received = received.lock().unwrap();
        let unique: HashSet<_> = received.iter().copied().collect();
        let sent = sent.load(Ordering::SeqCst);
        assert_eq!(received.len(), sent, "lost or duplicated messages");
        assert_eq!(unique.len(), sent, "duplicated messages");
        assert_eq!(drops.load(Ordering::SeqCst), sent, "drop count mismatch");
    }
}

#[test]
fn send_timeout_rendezvous_exactly_once() {
    for (senders, receivers, timeout_us) in [(4, 4, 50), (8, 2, 5), (2, 8, 200)] {
        let (tx, rx) = bounded::<Tracked>(0);
        let drops = Arc::new(AtomicUsize::new(0));
        let ok_ids = Arc::new(Mutex::new(Vec::new()));
        let failed = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let receiver_handles: Vec<_> = (0..receivers)
            .map(|_| {
                let rx = rx.clone();
                let received = received.clone();
                thread::spawn(move || {
                    let mut local = Vec::new();
                    while let Ok(item) = rx.recv() {
                        local.push(item.id);
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();
        drop(rx);
        let deadline = Instant::now() + RUN_FOR;
        let sender_handles: Vec<_> = (0..senders)
            .map(|s| {
                let tx = tx.clone();
                let drops = drops.clone();
                let ok_ids = ok_ids.clone();
                let failed = failed.clone();
                thread::spawn(move || {
                    let mut local = Vec::new();
                    let mut i = 0u64;
                    while Instant::now() < deadline {
                        let id = (s as u64) << 32 | i;
                        let item = Tracked {
                            id,
                            drops: drops.clone(),
                        };
                        match tx.send_timeout(item, Duration::from_micros(timeout_us)) {
                            Ok(()) => local.push(id),
                            Err(SendErrorTimeout::Timeout) => {
                                failed.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => panic!("spurious {e:?} on a live channel"),
                        }
                        i += 1;
                    }
                    ok_ids.lock().unwrap().extend(local);
                })
            })
            .collect();
        drop(tx);
        for h in sender_handles {
            h.join().unwrap();
        }
        for h in receiver_handles {
            h.join().unwrap();
        }
        let mut ok_ids = ok_ids.lock().unwrap().clone();
        let mut received = received.lock().unwrap().clone();
        ok_ids.sort_unstable();
        received.sort_unstable();
        assert_eq!(received, ok_ids, "Ok sends and receives differ");
        let total = ok_ids.len() + failed.load(Ordering::SeqCst);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            total,
            "every payload must be dropped exactly once (received or timed out)"
        );
    }
}

#[test]
fn send_option_timeout_rendezvous_exactly_once() {
    let (tx, rx) = bounded::<Tracked>(0);
    let drops = Arc::new(AtomicUsize::new(0));
    let receivers: Vec<_> = (0..4)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || {
                let mut n = 0usize;
                while rx.recv().is_ok() {
                    n += 1;
                }
                n
            })
        })
        .collect();
    drop(rx);
    let deadline = Instant::now() + RUN_FOR;
    let senders: Vec<_> = (0..4)
        .map(|s| {
            let tx = tx.clone();
            let drops = drops.clone();
            thread::spawn(move || {
                let (mut ok, mut kept) = (0usize, 0usize);
                let mut i = 0u64;
                while Instant::now() < deadline {
                    let mut data = Some(Tracked {
                        id: (s as u64) << 32 | i,
                        drops: drops.clone(),
                    });
                    match tx.send_option_timeout(&mut data, Duration::from_micros(30)) {
                        Ok(()) => {
                            assert!(data.is_none());
                            ok += 1;
                        }
                        Err(SendErrorTimeout::Timeout) => {
                            assert!(data.is_some(), "timed-out data must be handed back");
                            kept += 1;
                        }
                        Err(e) => panic!("spurious {e:?} on a live channel"),
                    }
                    drop(data);
                    i += 1;
                }
                (ok, kept)
            })
        })
        .collect();
    drop(tx);
    let (mut ok, mut kept) = (0, 0);
    for h in senders {
        let (o, k) = h.join().unwrap();
        ok += o;
        kept += k;
    }
    let received: usize = receivers.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(received, ok);
    assert_eq!(drops.load(Ordering::SeqCst), ok + kept);
}
