//! Scenario 4: disconnect / drop while peers are parked.

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Last sender dropped while consumers are parked in recv_timeout / recv / recv_async.
pub fn last_sender<const P: usize>(iters: u64, contended: bool) -> Outcome {
    let name = format!(
        "S4a last-sender drop vs parked recv_timeout(2s)x3 + recv_async x2 {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(600));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let rt = tokio_rt(2);
    let mut rng = Rng::new(4);
    let (mut bad_count, mut dup_or_miss, mut max_disc, mut wrong_err) = (0u64, 0u64, 0u64, 0u64);
    let mut total = 0u64;
    for it in 0..iters {
        let (tx, rx) = flume::unbounded::<Msg<P>>();
        let m = rng.below(50);
        let seen = Arc::new(Seen::new(m as usize + 1));
        let disc_at: Arc<Mutex<Vec<u64>>> = Arc::default();
        let errs = Arc::new(AtomicU64::new(0));
        let mut hs = vec![];
        for _ in 0..3 {
            let (rx, seen, disc_at, errs) = (rx.clone(), seen.clone(), disc_at.clone(), errs.clone());
            hs.push(std::thread::spawn(move || loop {
                match rx.recv_timeout(Duration::from_secs(2)) {
                    Ok(msg) => {
                        assert!(msg.intact());
                        seen.mark(msg.id);
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        // a correct channel never times out here: the sender is dropped
                        // within a few ms; count it and keep waiting
                        errs.fetch_add(1, Relaxed);
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        disc_at.lock().unwrap().push(now_ns());
                        break;
                    }
                }
            }));
        }
        let mut ts = vec![];
        for _ in 0..2 {
            let (rx, seen, disc_at) = (rx.clone(), seen.clone(), disc_at.clone());
            ts.push(rt.spawn(async move {
                while let Ok(msg) = rx.recv_async().await {
                    seen.mark(msg.id);
                }
                disc_at.lock().unwrap().push(now_ns());
            }));
        }
        drop(rx);
        if rng.below(2) == 0 {
            std::thread::sleep(Duration::from_micros(rng.below(2000)));
        }
        let half = m / 2;
        let tx2 = tx.clone();
        rt.block_on(async move {
            for id in 0..half {
                tx2.send_async(Msg::<P>::new(id, ctr)).await.unwrap();
            }
        });
        for id in half..m {
            tx.send(Msg::<P>::new(id, ctr)).unwrap();
        }
        if rng.below(2) == 0 {
            spin_until(now_ns() + rng.below(300_000));
        }
        let t_drop = now_ns();
        drop(tx);
        for h in hs {
            h.join().unwrap();
        }
        rt.block_on(async {
            for t in ts {
                t.await.unwrap();
            }
        });
        let (miss, dup) = seen.summary(m);
        total += m;
        if miss + dup > 0 {
            dup_or_miss += 1;
        }
        wrong_err += errs.load(SeqCst);
        let worst = disc_at.lock().unwrap().iter().map(|t| t.saturating_sub(t_drop)).max().unwrap_or(0);
        max_disc = max_disc.max(worst);
        if worst > 1_000_000_000 {
            bad_count += 1;
        }
        let _ = it;
    }
    drop(rt);
    let leak = ctr.live();
    let ok = dup_or_miss == 0 && leak == 0 && bad_count == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "iters={iters} msgs={total} iters-with-loss/dup={dup_or_miss} spurious-2s-timeouts={wrong_err} live_after={leak} max drop->Disconnected={:.3}ms iters-with-a-consumer-not-woken-by-disconnect(>1s)={bad_count}",
            ms(max_disc)
        ),
    )
}

/// Last receiver dropped while senders are blocked on a full bounded channel.
pub fn last_receiver<const P: usize>(iters: u64, contended: bool) -> Outcome {
    let name = format!(
        "S4b last-receiver drop vs blocked senders {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(600));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let rt = tokio_rt(1);
    let mut rng = Rng::new(5);
    let (mut wrong, mut max_lat) = (0u64, 0u64);
    for _ in 0..iters {
        let (tx, rx) = flume::bounded::<Msg<P>>(1);
        tx.send(Msg::new(0, ctr)).unwrap();
        let back: Arc<Mutex<Vec<(u64, u64)>>> = Arc::default(); // (id returned, time)
        let mut hs = vec![];
        {
            let (tx, back) = (tx.clone(), back.clone());
            hs.push(std::thread::spawn(move || {
                if let Err(flume::SendError(m)) = tx.send(Msg::<P>::new(1, ctr)) {
                    back.lock().unwrap().push((m.id, now_ns()));
                }
            }));
        }
        {
            let (tx, back) = (tx.clone(), back.clone());
            hs.push(std::thread::spawn(move || {
                if let Err(flume::SendTimeoutError::Disconnected(m)) =
                    tx.send_timeout(Msg::<P>::new(2, ctr), Duration::from_secs(20))
                {
                    back.lock().unwrap().push((m.id, now_ns()));
                }
            }));
        }
        let t = {
            let (tx, back) = (tx.clone(), back.clone());
            rt.spawn(async move {
                if let Err(flume::SendError(m)) = tx.send_async(Msg::<P>::new(3, ctr)).await {
                    back.lock().unwrap().push((m.id, now_ns()));
                }
            })
        };
        std::thread::sleep(Duration::from_micros(200 + rng.below(2000)));
        let t_drop = now_ns();
        drop(rx);
        for h in hs {
            h.join().unwrap();
        }
        rt.block_on(t).unwrap();
        let mut b = back.lock().unwrap().clone();
        b.sort();
        if b.iter().map(|x| x.0).collect::<Vec<_>>() != vec![1, 2, 3] {
            wrong += 1;
        }
        for (_, at) in b {
            max_lat = max_lat.max(at.saturating_sub(t_drop));
        }
        drop(tx);
    }
    drop(rt);
    let leak = ctr.live();
    let ok = wrong == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "iters={iters} iters-where-items-not-returned={wrong} live_after={leak} max drop->SendError={:.3}ms",
            ms(max_lat)
        ),
    )
}

/// WeakSender::upgrade racing with the last strong Sender drop.
pub fn weak_sender(iters: u64) -> Outcome {
    let name = "S4c WeakSender upgrade vs last drop";
    let _wd = watchdog(name, Duration::from_secs(600));
    let ctr = Counters::leak();
    let mut bad = 0;
    let mut rng = Rng::new(6);
    for _ in 0..iters {
        let (tx, rx) = flume::unbounded::<Small>();
        let weak = tx.downgrade();
        let sent = Arc::new(AtomicU64::new(0));
        let s2 = sent.clone();
        let w = std::thread::spawn(move || {
            let mut id = 0;
            while let Some(s) = weak.upgrade() {
                if s.send(Msg::new(id, ctr)).is_ok() {
                    id += 1;
                    s2.store(id, SeqCst);
                }
                if id >= 10_000 {
                    break;
                }
            }
        });
        let c = std::thread::spawn(move || {
            let mut got = vec![];
            while let Ok(m) = rx.recv() {
                got.push(m.id);
            }
            got
        });
        spin_until(now_ns() + rng.below(100_000));
        drop(tx);
        w.join().unwrap();
        let got = c.join().unwrap();
        let n = sent.load(SeqCst);
        if got != (0..n).collect::<Vec<_>>() {
            bad += 1;
        }
    }
    let leak = ctr.live();
    Outcome::new(
        name,
        bad == 0 && leak == 0,
        format!("iters={iters} iters-with-loss/reorder={bad} live_after={leak}"),
    )
}
