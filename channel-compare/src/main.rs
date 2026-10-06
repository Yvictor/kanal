//! Channel throughput/latency comparison: kanal vs crossbeam-channel vs flume
//! (sync) and kanal vs flume vs tokio::mpsc vs async-channel (async), in the
//! same shapes as kanal's published benchmarks, plus the shioaji pattern
//! (tokio task sends, OS thread drains with recv_timeout).
//!
//! Output: a Markdown table on stdout (median ns per message over ROUNDS
//! runs, and kanal / crossbeam ratio; < 1.00 means kanal is faster).

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

const MSGS: usize = 1 << 20;
const ROUNDS: usize = 5;

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct Big([usize; 4]); // kanal benchmarks use a 4-word struct

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct Tick([u64; 16]); // ~a parsed market-data tick (128 bytes)

trait Payload: Copy + Send + 'static {
    fn make(i: usize) -> Self;
}
impl Payload for usize {
    fn make(i: usize) -> Self {
        i + 1
    }
}
impl Payload for Big {
    fn make(i: usize) -> Self {
        Big([i + 1; 4])
    }
}
impl Payload for Tick {
    fn make(i: usize) -> Self {
        Tick([i as u64 + 1; 16])
    }
}

fn split(total: usize, parts: usize) -> Vec<usize> {
    (0..parts)
        .map(|p| total / parts + usize::from(p < total % parts))
        .collect()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Runs `f` (which moves MSGS messages and returns elapsed time) ROUNDS
/// times after one warm-up and returns median ns per message.
fn measure(mut f: impl FnMut() -> Duration) -> f64 {
    f();
    median(
        (0..ROUNDS)
            .map(|_| f().as_nanos() as f64 / MSGS as f64)
            .collect(),
    )
}

// ---------------------------------------------------------------- sync ----

macro_rules! sync_case {
    ($make:expr, $p:ty, $writers:expr, $readers:expr) => {
        measure(|| {
            let (tx, rx) = $make;
            let start = Instant::now();
            let mut hs = Vec::new();
            for n in split(MSGS, $readers) {
                let rx = rx.clone();
                hs.push(thread::spawn(move || {
                    for _ in 0..n {
                        black_box(rx.recv().unwrap());
                    }
                }));
            }
            for n in split(MSGS, $writers) {
                let tx = tx.clone();
                hs.push(thread::spawn(move || {
                    for i in 0..n {
                        tx.send(<$p>::make(i)).unwrap();
                    }
                }));
            }
            drop((tx, rx));
            for h in hs {
                h.join().unwrap();
            }
            start.elapsed()
        })
    };
}

/// Same-thread send then receive (only meaningful when capacity >= MSGS).
macro_rules! seq_case {
    ($make:expr, $p:ty) => {
        measure(|| {
            let (tx, rx) = $make;
            let start = Instant::now();
            for i in 0..MSGS {
                tx.send(<$p>::make(i)).unwrap();
            }
            for _ in 0..MSGS {
                black_box(rx.recv().unwrap());
            }
            start.elapsed()
        })
    };
}

fn sync_suite<P: Payload>(label: &str, rows: &mut Vec<Row>) {
    let cap_shapes: [(&str, Option<usize>); 4] = [
        ("bounded(0)", Some(0)),
        ("bounded(1)", Some(1)),
        ("bounded(50)", Some(50)),
        ("unbounded", None),
    ];
    let thread_shapes = [("spsc", 1, 1), ("mpsc 4x1", 4, 1), ("mpmc 4x4", 4, 4)];
    for (cap_name, cap) in cap_shapes {
        for (shape, w, r) in thread_shapes {
            let kanal = match cap {
                Some(c) => sync_case!(kanal::bounded::<P>(c), P, w, r),
                None => sync_case!(kanal::unbounded::<P>(), P, w, r),
            };
            let crossbeam = match cap {
                Some(c) => sync_case!(crossbeam_channel::bounded::<P>(c), P, w, r),
                None => sync_case!(crossbeam_channel::unbounded::<P>(), P, w, r),
            };
            let flume = match cap {
                Some(c) => sync_case!(flume::bounded::<P>(c), P, w, r),
                None => sync_case!(flume::unbounded::<P>(), P, w, r),
            };
            rows.push(Row::new(
                format!("sync {label} {cap_name} {shape}"),
                kanal,
                [("crossbeam", crossbeam), ("flume", flume)],
            ));
        }
    }
    rows.push(Row::new(
        format!("sync {label} unbounded seq"),
        seq_case!(kanal::unbounded::<P>(), P),
        [
            (
                "crossbeam",
                seq_case!(crossbeam_channel::unbounded::<P>(), P),
            ),
            ("flume", seq_case!(flume::unbounded::<P>(), P)),
        ],
    ));
}

/// Round-trip latency over two bounded(1) channels, ns per round trip.
fn pingpong_rows(rows: &mut Vec<Row>) {
    macro_rules! pp {
        ($make:expr) => {
            measure(|| {
                let (a_tx, a_rx) = $make;
                let (b_tx, b_rx) = $make;
                let echo = thread::spawn(move || {
                    for _ in 0..MSGS / 8 {
                        b_tx.send(a_rx.recv().unwrap()).unwrap();
                    }
                });
                let start = Instant::now();
                for i in 0..MSGS / 8 {
                    a_tx.send(i).unwrap();
                    black_box(b_rx.recv().unwrap());
                }
                let elapsed = start.elapsed();
                echo.join().unwrap();
                // measure() divides by MSGS; scale back to per round trip.
                elapsed * 8
            })
        };
    }
    rows.push(Row::new(
        "latency ping-pong bounded(1) (ns/round trip)".into(),
        pp!(kanal::bounded::<usize>(1)),
        [
            ("crossbeam", pp!(crossbeam_channel::bounded::<usize>(1))),
            ("flume", pp!(flume::bounded::<usize>(1))),
        ],
    ));
}

/// shioaji pattern: one tokio task produces, one OS thread drains with
/// recv_timeout(100ms).
fn shioaji_rows(rt: &tokio::runtime::Runtime, rows: &mut Vec<Row>) {
    let timeout = Duration::from_millis(100);
    let kanal = measure(|| {
        let (tx, rx) = kanal::unbounded_async::<Tick>();
        let rx = rx.clone_sync();
        let start = Instant::now();
        let h = rt.spawn(async move {
            for i in 0..MSGS {
                tx.send(Tick::make(i)).await.unwrap();
            }
        });
        for _ in 0..MSGS {
            black_box(rx.recv_timeout(timeout).unwrap());
        }
        rt.block_on(h).unwrap();
        start.elapsed()
    });
    let crossbeam = measure(|| {
        let (tx, rx) = crossbeam_channel::unbounded::<Tick>();
        let start = Instant::now();
        let h = rt.spawn(async move {
            for i in 0..MSGS {
                tx.send(Tick::make(i)).unwrap();
            }
        });
        for _ in 0..MSGS {
            black_box(rx.recv_timeout(timeout).unwrap());
        }
        rt.block_on(h).unwrap();
        start.elapsed()
    });
    let flume = measure(|| {
        let (tx, rx) = flume::unbounded::<Tick>();
        let start = Instant::now();
        let h = rt.spawn(async move {
            for i in 0..MSGS {
                tx.send_async(Tick::make(i)).await.unwrap();
            }
        });
        for _ in 0..MSGS {
            black_box(rx.recv_timeout(timeout).unwrap());
        }
        rt.block_on(h).unwrap();
        start.elapsed()
    });
    rows.push(Row::new(
        "shioaji: tokio task send -> thread recv_timeout, 128B".into(),
        kanal,
        [("crossbeam", crossbeam), ("flume", flume)],
    ));
}

// --------------------------------------------------------------- async ----

macro_rules! async_case {
    ($rt:expr, $make:expr, $p:ty, $writers:expr, $readers:expr, $send:ident, $recv:ident) => {
        measure(|| {
            let (tx, rx) = $make;
            let start = Instant::now();
            let mut hs = Vec::new();
            for n in split(MSGS, $readers) {
                let rx = rx.clone();
                hs.push($rt.spawn(async move {
                    for _ in 0..n {
                        black_box(rx.$recv().await.unwrap());
                    }
                }));
            }
            for n in split(MSGS, $writers) {
                let tx = tx.clone();
                hs.push($rt.spawn(async move {
                    for i in 0..n {
                        tx.$send(<$p>::make(i)).await.unwrap();
                    }
                }));
            }
            drop((tx, rx));
            for h in hs {
                $rt.block_on(h).unwrap();
            }
            start.elapsed()
        })
    };
}

/// tokio::mpsc is single-consumer; only spsc / mpsc shapes.
macro_rules! tokio_case {
    ($rt:expr, $cap:expr, $p:ty, $writers:expr) => {
        measure(|| {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<$p>($cap);
            let start = Instant::now();
            let reader = $rt.spawn(async move {
                for _ in 0..MSGS {
                    black_box(rx.recv().await.unwrap());
                }
            });
            let mut hs = Vec::new();
            for n in split(MSGS, $writers) {
                let tx = tx.clone();
                hs.push($rt.spawn(async move {
                    for i in 0..n {
                        tx.send(<$p>::make(i)).await.unwrap();
                    }
                }));
            }
            drop(tx);
            for h in hs {
                $rt.block_on(h).unwrap();
            }
            $rt.block_on(reader).unwrap();
            start.elapsed()
        })
    };
}

fn async_suite<P: Payload>(rt: &tokio::runtime::Runtime, label: &str, rows: &mut Vec<Row>) {
    for (cap_name, cap) in [("bounded(1)", 1usize), ("bounded(50)", 50)] {
        for (shape, w, r) in [("spsc", 1, 1), ("mpsc 4x1", 4, 1), ("mpmc 4x4", 4, 4)] {
            let kanal = async_case!(rt, kanal::bounded_async::<P>(cap), P, w, r, send, recv);
            let flume = async_case!(
                rt,
                flume::bounded::<P>(cap),
                P,
                w,
                r,
                send_async,
                recv_async
            );
            let async_ch = async_case!(rt, async_channel::bounded::<P>(cap), P, w, r, send, recv);
            let mut others = vec![("flume", flume), ("async-channel", async_ch)];
            if r == 1 {
                others.push(("tokio mpsc", tokio_case!(rt, cap, P, w)));
            }
            rows.push(Row::with_others(
                format!("async {label} {cap_name} {shape}"),
                kanal,
                others,
            ));
        }
    }
    for (shape, w, r) in [("spsc", 1, 1), ("mpmc 4x4", 4, 4)] {
        let kanal = async_case!(rt, kanal::unbounded_async::<P>(), P, w, r, send, recv);
        let flume = async_case!(rt, flume::unbounded::<P>(), P, w, r, send_async, recv_async);
        let async_ch = async_case!(rt, async_channel::unbounded::<P>(), P, w, r, send, recv);
        rows.push(Row::with_others(
            format!("async {label} unbounded {shape}"),
            kanal,
            vec![("flume", flume), ("async-channel", async_ch)],
        ));
    }
}

// -------------------------------------------------------------- output ----

struct Row {
    name: String,
    kanal: f64,
    others: Vec<(&'static str, f64)>,
}

impl Row {
    fn new<const N: usize>(name: String, kanal: f64, others: [(&'static str, f64); N]) -> Self {
        Self::with_others(name, kanal, others.to_vec())
    }
    fn with_others(name: String, kanal: f64, others: Vec<(&'static str, f64)>) -> Self {
        eprintln!("done: {name}");
        Self {
            name,
            kanal,
            others,
        }
    }
}

fn main() {
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .enable_all()
        .build()
        .unwrap();
    let mut rows = Vec::new();
    sync_suite::<usize>("usize", &mut rows);
    sync_suite::<Big>("big(32B)", &mut rows);
    pingpong_rows(&mut rows);
    shioaji_rows(&rt, &mut rows);
    async_suite::<usize>(&rt, "usize", &mut rows);
    async_suite::<Big>(&rt, "big(32B)", &mut rows);

    println!(
        "### {} / {} / {} cores\n",
        std::env::consts::OS,
        std::env::consts::ARCH,
        cores
    );
    println!(
        "Median of {ROUNDS} runs, {MSGS} messages. ns/msg unless noted. \
         Ratios < 1.00 mean kanal is faster.\n"
    );
    println!("| case | kanal | crossbeam | others | kanal / crossbeam | kanal / best other |");
    println!("| --- | ---: | ---: | --- | ---: | ---: |");
    let mut wins = 0;
    let (mut cb_cases, mut cb_wins) = (0, 0);
    for r in &rows {
        let crossbeam = r
            .others
            .iter()
            .find(|(n, _)| *n == "crossbeam")
            .map(|(_, v)| *v);
        let (best_name, best) = r
            .others
            .iter()
            .cloned()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .unwrap();
        let ratio = r.kanal / best;
        if ratio < 1.0 {
            wins += 1;
        }
        if let Some(cb) = crossbeam {
            cb_cases += 1;
            if r.kanal < cb {
                cb_wins += 1;
            }
        }
        let others = r
            .others
            .iter()
            .filter(|(n, _)| *n != "crossbeam")
            .map(|(n, v)| format!("{n} {v:.1}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "| {} | {:.1} | {} | {} | {} | {:.2} ({}) |",
            r.name,
            r.kanal,
            crossbeam.map_or("-".to_string(), |v| format!("{v:.1}")),
            others,
            crossbeam.map_or("-".to_string(), |v| format!("{:.2}", r.kanal / v)),
            ratio,
            best_name
        );
    }
    println!("\nkanal faster than crossbeam in {cb_wins} of {cb_cases} sync cases.");
    println!(
        "kanal fastest of all libraries in {wins} of {} cases.",
        rows.len()
    );
}
