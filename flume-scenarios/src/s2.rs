//! Scenario 2: MPMC with mixed consumers on one channel.
//!
//! Consumers: 2 OS threads `recv_timeout(100ms)`, 2 tokio tasks `recv_async`, each on a
//! cloned receiver. Producers: 2 tokio tasks `send_async`, 1 tokio task sync `send`
//! (dispatcher style; `send_async` for small bounded channels), 1 plain OS thread sync
//! `send` (Solace C-callback style). Oracles: exactly-once, integrity, no leaks; per
//! consumer/per producer order inversions are counted (characterisation, not failure).

use crate::common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const PRODUCERS: usize = 4;

#[derive(Default)]
struct ConsStats {
    got: u64,
    inversions: u64,
    corrupt: u64,
    last: [Option<u64>; PRODUCERS],
}

impl ConsStats {
    fn on<const P: usize>(&mut self, m: &Msg<P>, n: u64, seen: &Seen, lat: &Lat) {
        if !m.intact() {
            self.corrupt += 1;
        }
        seen.mark(m.id);
        lat.record(now_ns().saturating_sub(m.ts));
        self.got += 1;
        let (p, s) = ((m.id / n) as usize, m.id % n);
        if let Some(l) = self.last[p] {
            if s < l {
                self.inversions += 1;
            }
        }
        self.last[p] = Some(s);
    }
}

pub fn run<const P: usize>(n: u64, cap: Option<usize>, contended: bool) -> Outcome {
    let name = format!(
        "S2 mixed-MPMC {}B {}{}",
        std::mem::size_of::<Msg<P>>(),
        cap.map(|c| format!("bounded({c})")).unwrap_or("unbounded".into()),
        if contended { " +contention" } else { "" }
    );
    let _wd = watchdog(&name, Duration::from_secs(300));
    let _c = contend(contended);
    let ctr = Counters::leak();
    let (tx, rx) = match cap {
        Some(c) => flume::bounded::<Msg<P>>(c),
        None => flume::unbounded::<Msg<P>>(),
    };
    let seen = Arc::new(Seen::new((n * PRODUCERS as u64) as usize));
    let lat = Arc::new(Lat::default());
    let all: Arc<Mutex<Vec<(String, ConsStats)>>> = Arc::default();
    let rt = tokio_rt(2);

    // consumers
    let mut threads = vec![];
    for k in 0..2 {
        let (rx, seen, lat, all) = (rx.clone(), seen.clone(), lat.clone(), all.clone());
        threads.push(std::thread::spawn(move || {
            let mut st = ConsStats::default();
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(m) => st.on(&m, n, &seen, &lat),
                    Err(flume::RecvTimeoutError::Timeout) => {}
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
            }
            all.lock().unwrap().push((format!("thread{k}"), st));
        }));
    }
    let mut tasks = vec![];
    for k in 0..2 {
        let (rx, seen, lat, all) = (rx.clone(), seen.clone(), lat.clone(), all.clone());
        tasks.push(rt.spawn(async move {
            let mut st = ConsStats::default();
            while let Ok(m) = rx.recv_async().await {
                st.on(&m, n, &seen, &lat);
            }
            all.lock().unwrap().push((format!("task{k}"), st));
        }));
    }
    drop(rx);

    // producers
    let small_bounded = matches!(cap, Some(c) if c < 4096);
    for p in 0..3u64 {
        let tx = tx.clone();
        tasks.push(rt.spawn(async move {
            for s in 0..n {
                let m = Msg::<P>::new(p * n + s, ctr);
                if p == 2 && !small_bounded {
                    tx.send(m).unwrap();
                } else {
                    tx.send_async(m).await.unwrap();
                }
                if s % 64 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    {
        let tx = tx.clone();
        threads.push(std::thread::spawn(move || {
            for s in 0..n {
                tx.send(Msg::<P>::new(3 * n + s, ctr)).unwrap();
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
    drop(rt);
    let (miss, dup) = seen.summary(n * PRODUCERS as u64);
    let all = all.lock().unwrap();
    let corrupt: u64 = all.iter().map(|(_, s)| s.corrupt).sum();
    let inv: u64 = all.iter().map(|(_, s)| s.inversions).sum();
    let per: Vec<String> = all.iter().map(|(k, s)| format!("{k}:{}", s.got)).collect();
    let leak = ctr.live();
    let ok = miss == 0 && dup == 0 && corrupt == 0 && leak == 0;
    Outcome::new(
        name,
        ok,
        format!(
            "msgs={} missing={miss} dup={dup} corrupt={corrupt} live_after={leak} per-consumer[{}] same-producer order inversions seen by one consumer={inv} lat: {}",
            n * PRODUCERS as u64,
            per.join(" "),
            lat.describe()
        ),
    )
}
