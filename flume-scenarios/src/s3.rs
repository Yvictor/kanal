//! Scenario 3: cancellation of recv_async / send_async at every poll state.
//!
//! * `states_*`: deterministic manual-poll state machines (Miri-friendly).
//! * `recv_stress`: recv_async futures cancelled by tokio::time::timeout and select!,
//!   plus a long-lived future handed between two tasks (repolled with different
//!   wakers), plus an OS-thread recv_timeout consumer on the same channel.
//! * `send_stress`: send_async on a small bounded channel cancelled by timeout;
//!   each cancelled item must be either delivered exactly once or dropped unreceived.

use crate::common::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

fn check(results: &mut Vec<String>, ok: &mut bool, name: &str, cond: bool) {
    if !cond {
        *ok = false;
    }
    results.push(format!("{name}:{}", if cond { "ok" } else { "FAILED" }));
}

/// Deterministic RecvFut state machine.
pub fn states_recv() -> Outcome {
    let ctr = Counters::leak();
    let mut ok = true;
    let mut r = vec![];

    // R0: never polled, dropped.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        drop(rx.recv_async());
        tx.send(Msg::new(0, ctr)).unwrap();
        check(&mut r, &mut ok, "R0 drop-unpolled", rx.try_recv().map(|m| m.id) == Ok(0));
    }
    // R1: registered (Pending), dropped, then send.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let (_c, w) = count_waker();
        let mut f = rx.recv_async();
        let p = poll_once(&mut f, &w).is_pending();
        drop(f);
        tx.send(Msg::new(1, ctr)).unwrap();
        check(&mut r, &mut ok, "R1 drop-registered", p && rx.try_recv().map(|m| m.id) == Ok(1));
    }
    // R2: registered, woken by send, dropped before repoll -> message stays in channel and
    //     the next registered async receiver is woken.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let (ca, wa) = count_waker();
        let (cb, wb) = count_waker();
        let mut a = rx.recv_async();
        let mut b = rx.recv_async();
        assert!(poll_once(&mut a, &wa).is_pending());
        assert!(poll_once(&mut b, &wb).is_pending());
        tx.send(Msg::new(2, ctr)).unwrap();
        let a_woken = ca.0.load(SeqCst) == 1 && cb.0.load(SeqCst) == 0;
        drop(a);
        let b_woken = cb.0.load(SeqCst) >= 1;
        let got = poll_once(&mut b, &wb);
        check(
            &mut r,
            &mut ok,
            "R2 woken-then-dropped hands wake to next async",
            a_woken && b_woken && matches!(got, Poll::Ready(Ok(ref m)) if m.id == 2),
        );
    }
    // R3: repolled with a different waker; only the new waker must be woken.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let (c1, w1) = count_waker();
        let (c2, w2) = count_waker();
        let mut a = rx.recv_async();
        assert!(poll_once(&mut a, &w1).is_pending());
        assert!(poll_once(&mut a, &w2).is_pending());
        tx.send(Msg::new(3, ctr)).unwrap();
        let wk = c1.0.load(SeqCst) == 0 && c2.0.load(SeqCst) == 1;
        let got = poll_once(&mut a, &w2);
        check(
            &mut r,
            &mut ok,
            "R3 waker-replaced",
            wk && matches!(got, Poll::Ready(Ok(ref m)) if m.id == 3),
        );
    }
    // R4: woken, message stolen by try_recv, repolled with new waker, then next send wakes it.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let (_c1, w1) = count_waker();
        let (c2, w2) = count_waker();
        let mut a = rx.recv_async();
        assert!(poll_once(&mut a, &w1).is_pending());
        tx.send(Msg::new(4, ctr)).unwrap();
        let stolen = rx.try_recv().map(|m| m.id) == Ok(4);
        let p = poll_once(&mut a, &w2).is_pending();
        tx.send(Msg::new(5, ctr)).unwrap();
        let woke = c2.0.load(SeqCst) >= 1;
        let got = poll_once(&mut a, &w2);
        check(
            &mut r,
            &mut ok,
            "R4 stolen-then-repolled",
            stolen && p && woke && matches!(got, Poll::Ready(Ok(ref m)) if m.id == 5),
        );
    }
    // R5: disconnect while registered -> Ready(Err) after wake; buffered messages first.
    {
        let (tx, rx) = flume::unbounded::<Small>();
        let (c, w) = count_waker();
        let mut a = rx.recv_async();
        assert!(poll_once(&mut a, &w).is_pending());
        drop(tx);
        let woke = c.0.load(SeqCst) >= 1;
        let got = poll_once(&mut a, &w);
        check(&mut r, &mut ok, "R5 disconnect-wakes", woke && matches!(got, Poll::Ready(Err(_))));
    }
    let leak = ctr.live();
    check(&mut r, &mut ok, "no-leak", leak == 0);
    Outcome::new("S3a recv_async poll-state machine", ok, r.join(" "))
}

/// Deterministic SendFut state machine on bounded(1).
pub fn states_send() -> Outcome {
    let ctr = Counters::leak();
    let mut ok = true;
    let mut r = vec![];
    // T1: pending (hook queued) then dropped -> item dropped, not delivered.
    {
        let (tx, rx) = flume::bounded::<Small>(1);
        tx.send(Msg::new(0, ctr)).unwrap();
        let (_c, w) = count_waker();
        let before = ctr.dropped.load(SeqCst);
        let mut f = tx.send_async(Msg::new(1, ctr));
        let p = poll_once(&mut f, &w).is_pending();
        drop(f);
        let dropped_item = ctr.dropped.load(SeqCst) == before + 1;
        let a = rx.try_recv().map(|m| m.id);
        let b = rx.try_recv().map(|m| m.id);
        check(
            &mut r,
            &mut ok,
            "T1 cancel-pending-not-delivered",
            p && dropped_item && a == Ok(0) && b == Err(flume::TryRecvError::Empty),
        );
    }
    // T2: pending, receiver pulls it into the queue (wake), then the future is dropped
    //     without repoll: the item IS delivered although the sender saw a cancellation.
    {
        let (tx, rx) = flume::bounded::<Small>(1);
        tx.send(Msg::new(10, ctr)).unwrap();
        let (c, w) = count_waker();
        let mut f = tx.send_async(Msg::new(11, ctr));
        assert!(poll_once(&mut f, &w).is_pending());
        let a = rx.try_recv().map(|m| m.id);
        let woke = c.0.load(SeqCst) == 1;
        drop(f);
        let b = rx.try_recv().map(|m| m.id);
        let delivered = b == Ok(11);
        check(&mut r, &mut ok, "T2 woken-then-dropped", a == Ok(10) && woke && delivered);
        r.push("(T2: cancelled send_async can still deliver)".into());
    }
    // T3: repoll with a different waker.
    {
        let (tx, rx) = flume::bounded::<Small>(1);
        tx.send(Msg::new(20, ctr)).unwrap();
        let (c1, w1) = count_waker();
        let (c2, w2) = count_waker();
        let mut f = tx.send_async(Msg::new(21, ctr));
        assert!(poll_once(&mut f, &w1).is_pending());
        assert!(poll_once(&mut f, &w2).is_pending());
        let _ = rx.try_recv();
        let wk = c1.0.load(SeqCst) == 0 && c2.0.load(SeqCst) == 1;
        let done = matches!(poll_once(&mut f, &w2), Poll::Ready(Ok(())));
        let b = rx.try_recv().map(|m| m.id);
        check(&mut r, &mut ok, "T3 waker-replaced", wk && done && b == Ok(21));
    }
    // T4: last receiver dropped while pending -> Err(SendError(item)) returns the item.
    {
        let (tx, rx) = flume::bounded::<Small>(1);
        tx.send(Msg::new(30, ctr)).unwrap();
        let (c, w) = count_waker();
        let mut f = tx.send_async(Msg::new(31, ctr));
        assert!(poll_once(&mut f, &w).is_pending());
        drop(rx);
        let woke = c.0.load(SeqCst) >= 1;
        let got = poll_once(&mut f, &w);
        check(
            &mut r,
            &mut ok,
            "T4 rx-dropped-returns-item",
            woke && matches!(got, Poll::Ready(Err(flume::SendError(ref m))) if m.id == 31),
        );
    }
    let leak = ctr.live();
    check(&mut r, &mut ok, "no-leak", leak == 0);
    Outcome::new("S3b send_async poll-state machine", ok, r.join(" "))
}

/// Stress: cancelled recv_async futures (timeout / select! / handed between tasks)
/// mixed with an OS-thread recv_timeout consumer.
pub fn recv_stress<const P: usize>(n: u64, contended: bool) -> Outcome {
    let name = format!(
        "S3c recv_async cancel stress {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(300));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let (tx, rx) = flume::unbounded::<Msg<P>>();
    let seen = Arc::new(Seen::new(2 * n as usize));
    let lat_async = Arc::new(Lat::default());
    let lat_sync = Arc::new(Lat::default());
    let cancels = Arc::new(AtomicU64::new(0));
    let corrupt = Arc::new(AtomicU64::new(0));
    let rt = tokio_rt(3);
    let mut tasks = vec![];

    let on = |m: Msg<P>, seen: &Seen, lat: &Lat, corrupt: &AtomicU64| {
        if !m.intact() {
            corrupt.fetch_add(1, Relaxed);
        }
        seen.mark(m.id);
        lat.record(now_ns().saturating_sub(m.ts));
    };

    // timeout-cancelled consumers
    for k in 0..3u64 {
        let (rx, seen, lat, cancels, corrupt) =
            (rx.clone(), seen.clone(), lat_async.clone(), cancels.clone(), corrupt.clone());
        tasks.push(rt.spawn(async move {
            let mut rng = Rng::new(100 + k);
            loop {
                let d = Duration::from_micros(rng.below(300));
                match tokio::time::timeout(d, rx.recv_async()).await {
                    Ok(Ok(m)) => on(m, &seen, &lat, &corrupt),
                    Ok(Err(_)) => break,
                    Err(_) => {
                        cancels.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    // select!-cancelled consumers
    for k in 0..2u64 {
        let (rx, seen, lat, cancels, corrupt) =
            (rx.clone(), seen.clone(), lat_async.clone(), cancels.clone(), corrupt.clone());
        tasks.push(rt.spawn(async move {
            let mut rng = Rng::new(200 + k);
            loop {
                let spins = rng.below(4);
                tokio::select! {
                    r = rx.recv_async() => match r {
                        Ok(m) => on(m, &seen, &lat, &corrupt),
                        Err(_) => break,
                    },
                    _ = async { for _ in 0..spins { tokio::task::yield_now().await } } => {
                        cancels.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    // long-lived future handed back and forth between two tasks (different wakers)
    {
        type Fut<const P: usize> = Pin<Box<flume::r#async::RecvFut<'static, Msg<P>>>>;
        let (to_b, from_a) = tokio::sync::mpsc::unbounded_channel::<Fut<P>>();
        let (to_a, from_b) = tokio::sync::mpsc::unbounded_channel::<Fut<P>>();
        to_a.send(Box::pin(rx.clone().into_recv_async())).ok().unwrap();
        for (k, (mut inbox, outbox)) in [(from_b, to_b), (from_a, to_a)].into_iter().enumerate() {
            let (rx, seen, lat, cancels, corrupt) =
                (rx.clone(), seen.clone(), lat_async.clone(), cancels.clone(), corrupt.clone());
            tasks.push(rt.spawn(async move {
                let mut rng = Rng::new(300 + k as u64);
                while let Some(mut fut) = inbox.recv().await {
                    let d = Duration::from_micros(rng.below(200));
                    match tokio::time::timeout(d, &mut fut).await {
                        Ok(Ok(m)) => {
                            on(m, &seen, &lat, &corrupt);
                            fut = Box::pin(rx.clone().into_recv_async());
                        }
                        Ok(Err(_)) => break,
                        Err(_) => {
                            cancels.fetch_add(1, Relaxed);
                        }
                    }
                    if outbox.send(fut).is_err() {
                        break;
                    }
                }
            }));
        }
    }
    // sync consumer
    let sync_h = {
        let (rx, seen, lat, corrupt) = (rx.clone(), seen.clone(), lat_sync.clone(), corrupt.clone());
        std::thread::spawn(move || loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(m) => on(m, &seen, &lat, &corrupt),
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        })
    };
    drop(rx);
    // producers: tokio send_async with gaps, OS thread sync send with gaps
    {
        let tx = tx.clone();
        tasks.push(rt.spawn(async move {
            let mut rng = Rng::new(1);
            for s in 0..n {
                tx.send_async(Msg::<P>::new(s, ctr)).await.unwrap();
                match rng.below(100) {
                    0 => tokio::time::sleep(Duration::from_millis(1)).await,
                    1..=20 => tokio::task::yield_now().await,
                    _ => {}
                }
            }
        }));
    }
    let prod_h = {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(2);
            for s in 0..n {
                tx.send(Msg::<P>::new(n + s, ctr)).unwrap();
                let g = rng.below(100);
                if g < 10 {
                    spin_until(now_ns() + rng.below(200_000));
                }
            }
        })
    };
    drop(tx);
    prod_h.join().unwrap();
    rt.block_on(async {
        for t in tasks {
            t.await.unwrap();
        }
    });
    sync_h.join().unwrap();
    drop(rt);
    let (miss, dup) = seen.summary(2 * n);
    let corrupt = corrupt.load(SeqCst);
    let leak = ctr.live();
    let ok = miss == 0 && dup == 0 && corrupt == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "msgs={} missing={miss} dup={dup} corrupt={corrupt} live_after={leak} cancelled_polls={} | async lat: {} | sync recv_timeout lat: {}",
            2 * n,
            cancels.load(SeqCst),
            lat_async.describe(),
            lat_sync.describe()
        ),
    )
}

/// Stress: send_async on bounded(8) cancelled by timeout.
pub fn send_stress<const P: usize>(n: u64, contended: bool) -> Outcome {
    let name = format!(
        "S3d send_async cancel stress {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(300));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let (tx, rx) = flume::bounded::<Msg<P>>(8);
    let producers = 3u64;
    let seen = Arc::new(Seen::new((n * producers) as usize));
    // per id: 1 = send returned Ok, 2 = cancelled
    let status = Arc::new(Seen::new((n * producers) as usize));
    let rt = tokio_rt(3);
    let mut tasks = vec![];
    for p in 0..producers {
        let (tx, status) = (tx.clone(), status.clone());
        tasks.push(rt.spawn(async move {
            let mut rng = Rng::new(10 + p);
            for s in 0..n {
                let id = p * n + s;
                let d = Duration::from_micros(rng.below(2500));
                match tokio::time::timeout(d, tx.send_async(Msg::<P>::new(id, ctr))).await {
                    Ok(Ok(())) => {
                        status.mark(id);
                    }
                    Ok(Err(_)) => panic!("disconnected"),
                    Err(_) => {
                        status.mark(id);
                        status.mark(id);
                    }
                }
            }
        }));
    }
    drop(tx);
    let mut cons = vec![];
    for k in 0..2 {
        let (rx, seen) = (rx.clone(), seen.clone());
        cons.push(std::thread::spawn(move || {
            let mut rng = Rng::new(50 + k);
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(m) => {
                        assert!(m.intact());
                        seen.mark(m.id);
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {}
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
                // slow consumer so the bounded(8) channel is mostly full and sends park
                if rng.below(100) == 0 {
                    std::thread::sleep(Duration::from_millis(4));
                } else {
                    spin_until(now_ns() + 5_000 + rng.below(20_000));
                }
            }
        }));
    }
    {
        let (rx, seen) = (rx.clone(), seen.clone());
        tasks.push(rt.spawn(async move {
            while let Ok(m) = rx.recv_async().await {
                seen.mark(m.id);
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }));
    }
    drop(rx);
    rt.block_on(async {
        for t in tasks {
            t.await.unwrap();
        }
    });
    for h in cons {
        h.join().unwrap();
    }
    drop(rt);
    let (mut lost_ok, mut dup, mut canc, mut canc_deliv) = (0, 0, 0, 0);
    for id in 0..n * producers {
        let (st, c) = (status.count(id), seen.count(id));
        if c > 1 {
            dup += 1;
        }
        if st == 1 && c != 1 {
            lost_ok += 1;
        }
        if st == 2 {
            canc += 1;
            if c == 1 {
                canc_deliv += 1;
            }
        }
    }
    let leak = ctr.live();
    let ok = lost_ok == 0 && dup == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "msgs={} ok-send-but-lost={lost_ok} dup={dup} live_after={leak} cancelled={canc} cancelled-but-delivered={canc_deliv}",
            n * producers
        ),
    )
}

// Silence unused warning for Future import on some cfgs.
#[allow(dead_code)]
fn _f(_: &dyn Future<Output = ()>) {}
