//! Scenario 6: long soak in the shioaji topology.
//!
//! Unbounded channel; consumers: 2 OS threads recv_timeout(100ms) + 2 tokio tasks
//! recv_async (cloned receivers). Producers: a tokio "dispatcher" task paced at 10k msg/s
//! with send_async, and an OS thread (C-callback style) sending bursts of 5k msgs every
//! 100 ms with sync send. Per second: count, max latency, #>1ms, #>10ms, max queue depth.

use crate::common::*;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Sec {
    n: AtomicU64,
    max: AtomicU64,
    o1: AtomicU64,
    o10: AtomicU64,
    depth: AtomicU64,
}

pub fn run<const P: usize>(secs: u64, contended: bool, csv: Option<std::path::PathBuf>) -> Outcome {
    let name = format!(
        "S6 soak {secs}s 10k/s + 5k-burst/100ms {}B{}",
        std::mem::size_of::<Msg<P>>(),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(secs + 120));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let cap = (secs as usize + 3) * 61_000;
    let seen = Arc::new(Seen::new(cap));
    let lat = Arc::new(Lat::default());
    let per: Arc<Vec<Sec>> = Arc::new((0..secs + 30).map(|_| Sec::default()).collect());
    let corrupt = Arc::new(AtomicU64::new(0));
    let ids = Arc::new(AtomicU64::new(0));
    let (tx, rx) = flume::unbounded::<Msg<P>>();
    let rt = tokio_rt(2);
    let start = now_ns();
    let end = start + secs * 1_000_000_000;

    let on = {
        let (seen, lat, per, corrupt) = (seen.clone(), lat.clone(), per.clone(), corrupt.clone());
        move |m: Msg<P>| {
            let now = now_ns();
            if !m.intact() {
                corrupt.fetch_add(1, Relaxed);
            }
            seen.mark(m.id);
            let l = now.saturating_sub(m.ts);
            lat.record(l);
            let s = &per[(((now - start) / 1_000_000_000) as usize).min(per.len() - 1)];
            s.n.fetch_add(1, Relaxed);
            s.max.fetch_max(l, Relaxed);
            if l > 1_000_000 {
                s.o1.fetch_add(1, Relaxed);
            }
            if l > 10_000_000 {
                s.o10.fetch_add(1, Relaxed);
            }
        }
    };
    let mut threads = vec![];
    for _ in 0..2 {
        let (rx, on) = (rx.clone(), on.clone());
        threads.push(std::thread::spawn(move || loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(m) => on(m),
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        }));
    }
    let mut tasks = vec![];
    for _ in 0..2 {
        let (rx, on) = (rx.clone(), on.clone());
        tasks.push(rt.spawn(async move {
            while let Ok(m) = rx.recv_async().await {
                on(m);
            }
        }));
    }
    // depth monitor
    let mon = {
        let (rx, per) = (rx.clone(), per.clone());
        std::thread::spawn(move || {
            while now_ns() < end {
                let d = rx.len() as u64;
                let s = &per[(((now_ns() - start) / 1_000_000_000) as usize).min(per.len() - 1)];
                s.depth.fetch_max(d, Relaxed);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };
    drop(rx);
    {
        let (tx, ids) = (tx.clone(), ids.clone());
        tasks.push(rt.spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(1));
            let mut sent = 0u64;
            loop {
                iv.tick().await;
                let now = now_ns();
                if now >= end {
                    break;
                }
                let target = (now - start) / 100_000; // 10k/s
                while sent < target {
                    let id = ids.fetch_add(1, Relaxed);
                    tx.send_async(Msg::<P>::new(id, ctr)).await.unwrap();
                    sent += 1;
                }
            }
        }));
    }
    {
        let (tx, ids) = (tx.clone(), ids.clone());
        threads.push(std::thread::spawn(move || {
            let mut k = 1;
            loop {
                let t = start + k * 100_000_000;
                if t >= end {
                    break;
                }
                spin_until(t);
                for _ in 0..5000 {
                    let id = ids.fetch_add(1, Relaxed);
                    tx.send(Msg::<P>::new(id, ctr)).unwrap();
                }
                k += 1;
            }
        }));
    }
    drop(tx);
    rt.block_on(async {
        for t in tasks {
            t.await.unwrap();
        }
    });
    for h in threads {
        h.join().unwrap();
    }
    mon.join().unwrap();
    drop(rt);
    let total = ids.load(SeqCst);
    let (miss, dup) = seen.summary(total);
    let corrupt = corrupt.load(SeqCst);
    let leak = ctr.live();
    let mut worst_sec = (0u64, 0u64);
    let mut secs_o10 = 0;
    let mut out = String::from("sec,count,max_lat_us,over_1ms,over_10ms,max_queue_depth\n");
    for (i, s) in per.iter().enumerate().take(secs as usize + 1) {
        let mx = s.max.load(Relaxed);
        if mx > worst_sec.1 {
            worst_sec = (i as u64, mx);
        }
        if s.o10.load(Relaxed) > 0 {
            secs_o10 += 1;
        }
        out += &format!(
            "{i},{},{},{},{},{}\n",
            s.n.load(Relaxed),
            mx / 1000,
            s.o1.load(Relaxed),
            s.o10.load(Relaxed),
            s.depth.load(Relaxed)
        );
    }
    if let Some(p) = csv {
        if let Some(d) = p.parent() {
            let _ = std::fs::create_dir_all(d);
        }
        let _ = std::fs::File::create(&p).and_then(|mut f| f.write_all(out.as_bytes()));
    }
    let max_depth = per.iter().map(|s| s.depth.load(Relaxed)).max().unwrap_or(0);
    let ok = miss == 0 && dup == 0 && corrupt == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "msgs={total} missing={miss} dup={dup} corrupt={corrupt} live_after={leak} lat: {} | worst second #{} max={:.2}ms | seconds with any >10ms: {secs_o10} | max queue depth {max_depth}",
            lat.describe(),
            worst_sec.0,
            ms(worst_sec.1)
        ),
    )
}
