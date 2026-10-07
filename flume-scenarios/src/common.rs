//! Shared test machinery: drop-counting payloads, exactly-once oracle, latency
//! stats, CPU contention, hang watchdog, tiny RNG, counting wakers.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering::*};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Monotonic nanoseconds since the first call in this process.
pub fn now_ns() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Per-scenario creation/drop counters. Leaked so payloads can hold `&'static`.
#[derive(Default)]
pub struct Counters {
    pub created: AtomicU64,
    pub dropped: AtomicU64,
    /// If non-zero, dropping a payload whose `id % panic_mod == 0` panics.
    pub panic_mod: AtomicU64,
}

impl Counters {
    pub fn leak() -> &'static Counters {
        Box::leak(Box::default())
    }
    pub fn live(&self) -> i64 {
        self.created.load(SeqCst) as i64 - self.dropped.load(SeqCst) as i64
    }
}

/// Payload of `24 + P` bytes (rounded to 8): id, send timestamp, counters ref, padding.
pub struct Msg<const P: usize> {
    pub id: u64,
    pub ts: u64,
    ctr: &'static Counters,
    pad: [u8; P],
}

pub type Small = Msg<0>; // 24 B
pub type Medium = Msg<476>; // 504 B (~500 B)
pub type Large = Msg<1076>; // 1104 B (~1.1 KB)

const _: () = assert!(std::mem::size_of::<Small>() == 24);
const _: () = assert!(std::mem::size_of::<Medium>() == 504);
const _: () = assert!(std::mem::size_of::<Large>() == 1104);

impl<const P: usize> Msg<P> {
    pub fn new(id: u64, ctr: &'static Counters) -> Self {
        ctr.created.fetch_add(1, Relaxed);
        Msg { id, ts: now_ns(), ctr, pad: [id as u8; P] }
    }
    /// Cheap integrity check (detects use-after-free / torn copies of the payload).
    pub fn intact(&self) -> bool {
        if P == 0 {
            return self.ts <= now_ns();
        }
        let b = self.id as u8;
        self.pad[0] == b && self.pad[P / 2] == b && self.pad[P - 1] == b && self.ts <= now_ns()
    }
}

impl<const P: usize> std::fmt::Debug for Msg<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Msg#{}", self.id)
    }
}

impl<const P: usize> Drop for Msg<P> {
    fn drop(&mut self) {
        self.ctr.dropped.fetch_add(1, Relaxed);
        let m = self.ctr.panic_mod.load(Relaxed);
        if m != 0 && self.id % m == 0 && !std::thread::panicking() {
            panic!("payload Drop panic id={}", self.id);
        }
    }
}

/// Exactly-once oracle over a dense id space.
pub struct Seen(Vec<AtomicU8>);

impl Seen {
    pub fn new(n: usize) -> Self {
        Seen((0..n).map(|_| AtomicU8::new(0)).collect())
    }
    /// Returns the previous count (0 on first delivery).
    pub fn mark(&self, id: u64) -> u8 {
        self.0[id as usize].fetch_add(1, SeqCst)
    }
    pub fn count(&self, id: u64) -> u8 {
        self.0[id as usize].load(SeqCst)
    }
    /// (missing, duplicated) among ids `0..upto`.
    pub fn summary(&self, upto: u64) -> (u64, u64) {
        let (mut miss, mut dup) = (0, 0);
        for c in &self.0[..upto as usize] {
            match c.load(SeqCst) {
                0 => miss += 1,
                1 => {}
                _ => dup += 1,
            }
        }
        (miss, dup)
    }
}

/// Latency recorder (ns).
pub struct Lat {
    pub n: AtomicU64,
    pub max: AtomicU64,
    pub over1ms: AtomicU64,
    pub over10ms: AtomicU64,
    pub over50ms: AtomicU64,
    /// log2(µs) buckets for rough percentiles.
    pub bins: [AtomicU64; 40],
}

impl Default for Lat {
    fn default() -> Self {
        Lat {
            n: AtomicU64::new(0),
            max: AtomicU64::new(0),
            over1ms: AtomicU64::new(0),
            over10ms: AtomicU64::new(0),
            over50ms: AtomicU64::new(0),
            bins: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Lat {
    pub fn record(&self, ns: u64) {
        self.n.fetch_add(1, Relaxed);
        self.max.fetch_max(ns, Relaxed);
        if ns > 1_000_000 {
            self.over1ms.fetch_add(1, Relaxed);
        }
        if ns > 10_000_000 {
            self.over10ms.fetch_add(1, Relaxed);
        }
        if ns > 50_000_000 {
            self.over50ms.fetch_add(1, Relaxed);
        }
        let us = ns / 1000;
        let b = (64 - us.leading_zeros()) as usize;
        self.bins[b.min(39)].fetch_add(1, Relaxed);
    }
    /// Upper bound (µs) of the bucket containing quantile q.
    pub fn quantile_us(&self, q: f64) -> u64 {
        let n = self.n.load(Relaxed);
        if n == 0 {
            return 0;
        }
        let target = ((n as f64) * q).ceil() as u64;
        let mut acc = 0;
        for (i, b) in self.bins.iter().enumerate() {
            acc += b.load(Relaxed);
            if acc >= target {
                return if i == 0 { 1 } else { 1u64 << i };
            }
        }
        u64::MAX
    }
    pub fn describe(&self) -> String {
        format!(
            "n={} max={:.3}ms p99<={}us p99.9<={}us p99.99<={}us >1ms={} >10ms={} >50ms={}",
            self.n.load(Relaxed),
            self.max.load(Relaxed) as f64 / 1e6,
            self.quantile_us(0.99),
            self.quantile_us(0.999),
            self.quantile_us(0.9999),
            self.over1ms.load(Relaxed),
            self.over10ms.load(Relaxed),
            self.over50ms.load(Relaxed)
        )
    }
}

/// One busy-loop thread per core while alive.
pub struct Contention {
    stop: Arc<AtomicBool>,
    hs: Vec<JoinHandle<()>>,
}

pub fn contend(on: bool) -> Option<Contention> {
    if !on {
        return None;
    }
    let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let stop = Arc::new(AtomicBool::new(false));
    let hs = (0..n)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut x = 0u64;
                while !stop.load(Relaxed) {
                    for _ in 0..1000 {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                        std::hint::black_box(x);
                    }
                }
            })
        })
        .collect();
    Some(Contention { stop, hs })
}

impl Drop for Contention {
    fn drop(&mut self) {
        self.stop.store(true, Relaxed);
        for h in self.hs.drain(..) {
            let _ = h.join();
        }
    }
}

/// Aborts the process with a message if not dropped within `limit` (hang detector).
pub struct Watchdog {
    done: Option<std::sync::mpsc::Sender<()>>,
    h: Option<JoinHandle<()>>,
}

pub fn watchdog(name: &str, limit: Duration) -> Watchdog {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let name = name.to_string();
    let h = std::thread::spawn(move || {
        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(limit) {
            eprintln!("HANG: scenario `{name}` exceeded {limit:?}; aborting");
            println!("| {name} | **HANG** | exceeded {limit:?} |");
            std::process::exit(3);
        }
    });
    Watchdog { done: Some(tx), h: Some(h) }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.done.take());
        if let Some(h) = self.h.take() {
            let _ = h.join();
        }
    }
}

/// xorshift64*
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

/// Waker that counts wake-ups.
#[derive(Default)]
pub struct CountWaker(pub AtomicUsize);
impl Wake for CountWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, SeqCst);
    }
}
pub fn count_waker() -> (Arc<CountWaker>, Waker) {
    let a = Arc::new(CountWaker::default());
    (a.clone(), Waker::from(a))
}

pub fn poll_once<F: std::future::Future + Unpin>(f: &mut F, w: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(w);
    std::pin::Pin::new(f).poll(&mut cx)
}

/// Result line of a scenario.
pub struct Outcome {
    pub name: String,
    pub ok: bool,
    pub status: String,
    pub detail: String,
}

impl Outcome {
    pub fn new(name: impl Into<String>, ok: bool, detail: impl Into<String>) -> Self {
        let status = if ok { "PASS" } else { "**FAIL**" }.to_string();
        Outcome { name: name.into(), ok, status, detail: detail.into() }
    }
    /// A known-defect reproduction: never fails the run, reports whether it reproduced.
    pub fn repro(name: impl Into<String>, reproduced: bool, detail: impl Into<String>) -> Self {
        let status = if reproduced { "**BUG REPRODUCED**" } else { "not reproduced" }.to_string();
        Outcome { name: name.into(), ok: true, status, detail: detail.into() }
    }
    pub fn print(&self) {
        println!("| {} | {} | {} |", self.name, self.status, self.detail);
    }
}

pub fn spin_until(t_ns: u64) {
    let now = now_ns();
    if t_ns > now + 2_000_000 {
        std::thread::sleep(Duration::from_nanos(t_ns - now - 2_000_000));
    }
    while now_ns() < t_ns {
        std::hint::spin_loop();
    }
}

pub fn tokio_rt(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_time()
        .build()
        .unwrap()
}

pub fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}
