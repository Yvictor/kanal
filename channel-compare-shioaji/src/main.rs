//! Shioaji-shaped channel comparison: kanal (current) vs crossbeam-channel,
//! tokio::sync (mpsc / broadcast), flume and async-channel.
//!
//! Usage map (shioaji gitlab-shioaji/beta 902f13d4, rsolace v0.3.13 `channel`):
//!
//! | path | sender | channel | receiver(s) | payload |
//! |---|---|---|---|---|
//! | P3 msg/p2p | Solace C callback OS thread, sync `send` | rsolace `kanal::unbounded()` -> `as_async()` | tokio dispatcher / p2p-dispatch task `recv().await` | SolMsg 24 B |
//! | P4 session events | C callback OS thread | rsolace `kanal::unbounded()` | Python event thread `clone_sync().recv_timeout(100ms)`; login wait (`clone_sync`), server/maintenance `recv().await` -> may compete | SolEvent 48 B |
//! | P1/P6 tick, bidask, quote stk/fop/idx | dispatcher task `send().await` | `kanal::unbounded_async()` per route | Python callback thread `clone_sync().recv_timeout(100ms)`; Python asyncio `get_*_receiver().recv()` (same channel, competing: P7); server forwarding task `recv().await` (P2) | 392-1104 B |
//! | derived (calc index, contributions, components, kbar, scanner) | dispatcher task | `kanal::bounded_async(65536)` | as above | 104-784 B |
//! | order events | p2p dispatch task (after TradeCache) | `kanal::unbounded_async()` | Python order thread `recv_timeout`; Python receiver; server forwarding | OrderEvent 520 B |
//! | sys contract | dispatcher task | `kanal::unbounded_async()` | Python contract thread `recv_timeout`; login wait | UpdateContract 152 B |
//! | server fan-out | forwarding task `recv().await` | tokio `broadcast(1000)` | per-SSE-connection `BroadcastStream` | standard types (392 B for BidAskSTKv1) |
//!
//! Each payload carries the send `Instant` (ns since process start) so the
//! consumer records end-to-end latency into an HDR histogram, a per-second
//! max series, threshold counts and the top spikes.
//!
//! Usage: channel-compare-shioaji [--group A1|A2|B|C|all] [--contend]
//!        [--long-secs N] [--out DIR] [--quick] [--kanal-only] [--reverse]
//!        [--tag SUFFIX]

use std::hint::black_box;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use cpu_time::ProcessTime;
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, mpsc};

// ---------------------------------------------------------------------------
// Clock and payloads
// ---------------------------------------------------------------------------

static BASE: OnceLock<Instant> = OnceLock::new();

#[inline]
fn now_ns() -> u64 {
    BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Payload of exactly `16 + 8 * W` bytes: send timestamp, sequence, padding.
#[derive(Clone, Copy)]
#[repr(C)]
struct P<const W: usize> {
    t: u64,
    seq: u64,
    pad: [u64; W],
}

impl<const W: usize> P<W> {
    #[inline]
    fn new(seq: u64, tag: u64) -> Self {
        P { t: now_ns(), seq, pad: [tag; W] }
    }
    #[inline]
    fn from_t(t: u64, seq: u64) -> Self {
        P { t, seq, pad: [seq; W] }
    }
}

// Sizes measured with size_of on shioaji gitlab-shioaji/beta (902f13d4),
// python build (feature optimized-parser), rsolace v0.3.13.
type SolMsg = P<1>; // rsolace::solmsg::SolMsg            24 B
type SolEvent = P<4>; // rsolace::solevent::SolEvent       48 B
type TickStkCh = P<60>; // ChannelTickSTKv1 (optimized)    496 B
type BidAskStkCh = P<75>; // ChannelBidAskSTKv1 (optimized) 616 B
type TickFopCh = P<47>; // ChannelTickFOPv1 (optimized)    392 B
type BidAskFopCh = P<91>; // ChannelBidAskFOPv1 (optimized) 744 B
type QuoteStkCh = P<136>; // ChannelQuoteSTKv1 (optimized) 1104 B
type QuoteFopCh = P<132>; // ChannelQuoteFOPv1 (optimized) 1072 B
type QuoteIdxCh = P<136>; // ChannelQuoteIdxV1             1104 B
type OrderEvent = P<63>; // OrderEvent                     520 B
type BidAskStk = P<47>; // BidAskSTKv1 (server broadcast)  392 B

const _: () = {
    use std::mem::size_of;
    assert!(size_of::<SolMsg>() == 24);
    assert!(size_of::<SolEvent>() == 48);
    assert!(size_of::<TickStkCh>() == 496);
    assert!(size_of::<BidAskStkCh>() == 616);
    assert!(size_of::<TickFopCh>() == 392);
    assert!(size_of::<BidAskFopCh>() == 744);
    assert!(size_of::<QuoteStkCh>() == 1104);
    assert!(size_of::<QuoteFopCh>() == 1072);
    assert!(size_of::<QuoteIdxCh>() == 1104);
    assert!(size_of::<OrderEvent>() == 520);
    assert!(size_of::<BidAskStk>() == 392);
};

trait Stamp: Send + Clone + 'static {
    fn t(&self) -> u64;
}
impl<const W: usize> Stamp for P<W> {
    #[inline]
    fn t(&self) -> u64 {
        self.t
    }
}

// ---------------------------------------------------------------------------
// Latency recorder
// ---------------------------------------------------------------------------

struct Rec {
    h: Histogram<u64>,
    sec_max: Vec<u64>,
    top: Vec<(u64, u64)>,
    start: u64,
    last: u64,
}

impl Rec {
    fn new(start: u64) -> Self {
        Rec {
            h: Histogram::new_with_bounds(1, 300_000_000_000, 3).unwrap(),
            sec_max: Vec::with_capacity(512),
            top: Vec::with_capacity(8),
            start,
            last: 0,
        }
    }

    #[inline]
    fn rec(&mut self, t: u64) {
        let now = now_ns();
        let lat = now.saturating_sub(t).max(1);
        self.h.saturating_record(lat);
        self.last = now;
        let s = (t.saturating_sub(self.start) / 1_000_000_000) as usize;
        if s >= self.sec_max.len() {
            self.sec_max.resize(s + 1, 0);
        }
        if lat > self.sec_max[s] {
            self.sec_max[s] = lat;
        }
        if self.top.len() < 5 || lat > self.top[self.top.len() - 1].0 {
            self.push_top(lat, t.saturating_sub(self.start));
        }
    }

    fn push_top(&mut self, lat: u64, at: u64) {
        self.top.push((lat, at));
        self.top.sort_by(|a, b| b.0.cmp(&a.0));
        self.top.truncate(5);
    }

    fn merge(mut self, o: Rec) -> Rec {
        self.h.add(&o.h).unwrap();
        if o.sec_max.len() > self.sec_max.len() {
            self.sec_max.resize(o.sec_max.len(), 0);
        }
        for (i, v) in o.sec_max.iter().enumerate() {
            self.sec_max[i] = self.sec_max[i].max(*v);
        }
        for (l, a) in o.top {
            self.push_top(l, a);
        }
        self.last = self.last.max(o.last);
        self
    }
}

#[derive(Serialize, Clone, Default)]
struct Stats {
    n: u64,
    p50: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
    max: f64,
    mean: f64,
    stdev: f64,
    over_100us: u64,
    over_1ms: u64,
    over_10ms: u64,
    secs: usize,
    secs_max_over_1ms: usize,
    /// per-second max latency (µs), bucketed by send time
    sec_max_us: Vec<f64>,
    /// top spikes: (latency µs, send offset s)
    top: Vec<(f64, f64)>,
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1000.0
}

fn stats(r: &Rec) -> Stats {
    let h = &r.h;
    let n = h.len();
    if n == 0 {
        return Stats::default();
    }
    let over = |th: u64| n - h.count_between(1, th);
    Stats {
        n,
        p50: us(h.value_at_quantile(0.5)),
        p99: us(h.value_at_quantile(0.99)),
        p999: us(h.value_at_quantile(0.999)),
        p9999: us(h.value_at_quantile(0.9999)),
        max: us(h.max()),
        mean: h.mean() / 1000.0,
        stdev: h.stdev() / 1000.0,
        over_100us: over(100_000),
        over_1ms: over(1_000_000),
        over_10ms: over(10_000_000),
        secs: r.sec_max.len(),
        secs_max_over_1ms: r.sec_max.iter().filter(|&&v| v > 1_000_000).count(),
        sec_max_us: r.sec_max.iter().map(|&v| us(v)).collect(),
        top: r.top.iter().map(|&(l, a)| (us(l), a as f64 / 1e9)).collect(),
    }
}

// ---------------------------------------------------------------------------
// Channel abstraction (enum dispatch; identical overhead for every library)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lib {
    Kanal,
    /// the same kanal sources built with feature `std-mutex` (crate kanal_std)
    KanalStd,
    Crossbeam,
    Tokio,
    Flume,
    AsyncChan,
    /// crossbeam for the sync consumer + bridge thread into tokio mpsc for the
    /// async consumer (both compete on the same crossbeam queue)
    CbBridge,
    /// tokio::sync::broadcast: every consumer gets every message
    Broadcast,
}

const BCAST_CAP: usize = 65_536;
static LAGGED: AtomicU64 = AtomicU64::new(0);
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

enum Tx<T> {
    KA(kanal::AsyncSender<T>),
    KS(kanal::Sender<T>),
    KA2(kanal_std::AsyncSender<T>),
    KS2(kanal_std::Sender<T>),
    Cb(crossbeam_channel::Sender<T>),
    Tk(mpsc::UnboundedSender<T>),
    Fl(flume::Sender<T>),
    Ac(async_channel::Sender<T>),
    Bc(broadcast::Sender<T>),
}

impl<T: Stamp> Tx<T> {
    /// Send from a plain OS thread (Solace C callback context).
    #[inline]
    fn send_sync(&self, v: T) -> bool {
        match self {
            Tx::KA(s) => s.as_sync().send(v).is_ok(),
            Tx::KS(s) => s.send(v).is_ok(),
            Tx::KA2(s) => s.as_sync().send(v).is_ok(),
            Tx::KS2(s) => s.send(v).is_ok(),
            Tx::Cb(s) => s.send(v).is_ok(),
            Tx::Tk(s) => s.send(v).is_ok(),
            Tx::Fl(s) => s.send(v).is_ok(),
            Tx::Ac(s) => s.try_send(v).is_ok(),
            Tx::Bc(s) => {
                let _ = s.send(v);
                true
            }
        }
    }

    /// Send from a tokio task (shioaji dispatcher). kanal keeps the current
    /// `AsyncSender::send(v).await`; unbounded alternatives never block.
    #[inline]
    async fn send_async(&self, v: T) -> bool {
        match self {
            Tx::KA(s) => s.send(v).await.is_ok(),
            Tx::KA2(s) => s.send(v).await.is_ok(),
            _ => self.send_sync(v),
        }
    }
}

enum Rx<T> {
    KA(kanal::AsyncReceiver<T>),
    KS(kanal::Receiver<T>),
    KA2(kanal_std::AsyncReceiver<T>),
    KS2(kanal_std::Receiver<T>),
    Cb(crossbeam_channel::Receiver<T>),
    Tk(mpsc::UnboundedReceiver<T>),
    Fl(flume::Receiver<T>),
    Ac(async_channel::Receiver<T>),
    Bc(broadcast::Receiver<T>),
}

enum R<T> {
    Msg(T),
    Timeout,
    Closed,
}

impl<T: Stamp> Rx<T> {
    /// shioaji Python handler threads do `clone_sync()` of the core AsyncReceiver.
    fn to_sync(self) -> Rx<T> {
        match self {
            Rx::KA(r) => Rx::KS(r.clone_sync()),
            Rx::KA2(r) => Rx::KS2(r.clone_sync()),
            other => other,
        }
    }

    /// Another competing consumer of the same channel (MPMC).
    fn dup(&self) -> Rx<T> {
        match self {
            Rx::KA(r) => Rx::KA(r.clone()),
            Rx::KS(r) => Rx::KS(r.clone()),
            Rx::KA2(r) => Rx::KA2(r.clone()),
            Rx::KS2(r) => Rx::KS2(r.clone()),
            Rx::Cb(r) => Rx::Cb(r.clone()),
            Rx::Fl(r) => Rx::Fl(r.clone()),
            Rx::Ac(r) => Rx::Ac(r.clone()),
            Rx::Bc(r) => Rx::Bc(r.resubscribe()),
            Rx::Tk(_) => panic!("tokio mpsc is single-consumer"),
        }
    }

    /// Blocking receive with timeout (Python callback thread). async-channel,
    /// tokio mpsc and broadcast have no timed blocking receive: they block
    /// until a message or close (shutdown then needs the channel closed).
    #[inline]
    fn recv_timeout(&mut self, d: Duration) -> R<T> {
        match self {
            Rx::KS(r) => match r.recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(kanal::ReceiveErrorTimeout::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::KA(r) => match r.as_sync().recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(kanal::ReceiveErrorTimeout::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::KS2(r) => match r.recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(kanal_std::ReceiveErrorTimeout::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::KA2(r) => match r.as_sync().recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(kanal_std::ReceiveErrorTimeout::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::Cb(r) => match r.recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::Fl(r) => match r.recv_timeout(d) {
                Ok(v) => R::Msg(v),
                Err(flume::RecvTimeoutError::Timeout) => R::Timeout,
                Err(_) => R::Closed,
            },
            Rx::Ac(r) => match r.recv_blocking() {
                Ok(v) => R::Msg(v),
                Err(_) => R::Closed,
            },
            Rx::Tk(r) => match r.blocking_recv() {
                Some(v) => R::Msg(v),
                None => R::Closed,
            },
            Rx::Bc(r) => match r.blocking_recv() {
                Ok(v) => R::Msg(v),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    LAGGED.fetch_add(n, Relaxed);
                    R::Timeout
                }
                Err(_) => R::Closed,
            },
        }
    }

    #[inline]
    async fn recv_async(&mut self) -> Option<T> {
        match self {
            Rx::KA(r) => r.recv().await.ok(),
            Rx::KA2(r) => r.recv().await.ok(),
            Rx::Tk(r) => r.recv().await,
            Rx::Fl(r) => r.recv_async().await.ok(),
            Rx::Ac(r) => r.recv().await.ok(),
            Rx::Bc(r) => loop {
                match r.recv().await {
                    Ok(v) => return Some(v),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        LAGGED.fetch_add(n, Relaxed);
                    }
                    Err(_) => return None,
                }
            },
            Rx::KS(_) | Rx::KS2(_) | Rx::Cb(_) => panic!("sync-only receiver used from async"),
        }
    }
}

/// Channel created by shioaji core and fed from the tokio dispatcher task.
fn route<T: Stamp>(lib: Lib) -> (Tx<T>, Rx<T>) {
    match lib {
        Lib::Kanal => {
            let (s, r) = kanal::unbounded_async();
            (Tx::KA(s), Rx::KA(r))
        }
        Lib::KanalStd => {
            let (s, r) = kanal_std::unbounded_async();
            (Tx::KA2(s), Rx::KA2(r))
        }
        Lib::Crossbeam | Lib::CbBridge => {
            let (s, r) = crossbeam_channel::unbounded();
            (Tx::Cb(s), Rx::Cb(r))
        }
        Lib::Tokio => {
            let (s, r) = mpsc::unbounded_channel();
            (Tx::Tk(s), Rx::Tk(r))
        }
        Lib::Flume => {
            let (s, r) = flume::unbounded();
            (Tx::Fl(s), Rx::Fl(r))
        }
        Lib::AsyncChan => {
            let (s, r) = async_channel::unbounded();
            (Tx::Ac(s), Rx::Ac(r))
        }
        Lib::Broadcast => {
            let (s, r) = broadcast::channel(BCAST_CAP);
            (Tx::Bc(s), Rx::Bc(r))
        }
    }
}

/// Channel created by rsolace and fed from the Solace C callback thread:
/// kanal sync `unbounded()` whose receiver is handed out as `as_async().clone()`.
fn cthread<T: Stamp>(lib: Lib) -> (Tx<T>, Rx<T>) {
    match lib {
        Lib::Kanal => {
            let (s, r) = kanal::unbounded();
            (Tx::KS(s), Rx::KA(r.as_async().clone()))
        }
        Lib::KanalStd => {
            let (s, r) = kanal_std::unbounded();
            (Tx::KS2(s), Rx::KA2(r.as_async().clone()))
        }
        other => route(other),
    }
}

// ---------------------------------------------------------------------------
// Traffic shapes
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// saturated: send n messages back to back
    Flood { n: u64 },
    /// `rate` msg/s, catch-up every ~1 ms (Solace-like micro batches)
    Paced { rate: u64, secs: f64 },
    /// `burst` messages back to back every `every_ms` (market open)
    Bursty { burst: u64, every_ms: u64, secs: f64 },
    /// no traffic: idle cost of the consumers
    Idle { secs: f64 },
}

macro_rules! pace {
    ($mode:expr, $seq:ident => $send:block, $d:ident => $sleep:block) => {{
        let start = Instant::now();
        let mut $seq: u64 = 0;
        match $mode {
            Mode::Flood { n } => {
                while $seq < n {
                    $send;
                    $seq += 1;
                }
            }
            Mode::Paced { rate, secs } => loop {
                let el = start.elapsed().as_secs_f64();
                if el >= secs {
                    break;
                }
                let target = (el * rate as f64) as u64;
                while $seq < target {
                    $send;
                    $seq += 1;
                }
                let $d = Duration::from_millis(1);
                $sleep;
            },
            Mode::Bursty { burst, every_ms, secs } => {
                let mut k = 0u64;
                loop {
                    let due = Duration::from_millis(every_ms * k);
                    if due.as_secs_f64() >= secs {
                        break;
                    }
                    let el = start.elapsed();
                    if due > el {
                        let $d = due - el;
                        $sleep;
                    }
                    for _ in 0..burst {
                        $send;
                        $seq += 1;
                    }
                    k += 1;
                }
            }
            Mode::Idle { secs } => {
                let $d = Duration::from_secs_f64(secs);
                $sleep;
            }
        }
        $seq
    }};
}

fn pace_sync<T: Stamp>(mode: Mode, tx: Tx<T>, make: impl Fn(u64) -> T) -> u64 {
    pace!(mode, seq => { tx.send_sync(make(seq)); }, d => { thread::sleep(d) })
}

async fn pace_task<T: Stamp>(mode: Mode, tx: Tx<T>, make: impl Fn(u64) -> T) -> u64 {
    pace!(mode, seq => { tx.send_async(make(seq)).await; }, d => { tokio::time::sleep(d).await })
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

/// Python callback thread: `loop { shutdown?; recv_timeout(100ms) }`.
fn sync_consumer<T: Stamp>(mut rx: Rx<T>, start: u64) -> thread::JoinHandle<Rec> {
    thread::spawn(move || {
        let mut rec = Rec::new(start);
        loop {
            if SHUTDOWN.load(Relaxed) {
                break;
            }
            match rx.recv_timeout(Duration::from_millis(100)) {
                R::Msg(v) => rec.rec(black_box(v).t()),
                R::Timeout => continue,
                R::Closed => break,
            }
        }
        rec
    })
}

/// tokio task doing `while let Ok(v) = rx.recv().await`.
fn async_consumer<T: Stamp>(rt: &Runtime, mut rx: Rx<T>, start: u64) -> tokio::task::JoinHandle<Rec> {
    rt.spawn(async move {
        let mut rec = Rec::new(start);
        while let Some(v) = rx.recv_async().await {
            rec.rec(black_box(v).t());
        }
        rec
    })
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

#[derive(Serialize, Clone, Default)]
struct Run {
    sent: u64,
    delivered: u64,
    /// first send to last receive
    secs: f64,
    mps: f64,
    cpu_ns_per_msg: f64,
    /// process CPU time / wall time (100 = one core busy)
    cpu_pct: f64,
    lagged: u64,
    /// messages per consumer (MPMC split)
    split: Vec<u64>,
    lat: Stats,
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

fn harness(f: impl FnOnce(&Runtime, u64) -> (u64, Vec<Rec>)) -> Run {
    let rt = runtime();
    LAGGED.store(0, Relaxed);
    let cpu0 = ProcessTime::now();
    let start = now_ns();
    let (sent, recs) = f(&rt, start);
    let wall = (now_ns() - start) as f64;
    let cpu = cpu0.elapsed().as_nanos() as f64;
    rt.shutdown_timeout(Duration::from_secs(1));
    let split: Vec<u64> = recs.iter().map(|r| r.h.len()).collect();
    let rec = recs.into_iter().reduce(Rec::merge).unwrap();
    let delivered = rec.h.len();
    let secs = if delivered > 0 { (rec.last - start) as f64 / 1e9 } else { wall / 1e9 };
    Run {
        sent,
        delivered,
        secs,
        mps: delivered as f64 / secs,
        cpu_ns_per_msg: if delivered > 0 { cpu / delivered as f64 } else { 0.0 },
        cpu_pct: cpu / wall * 100.0,
        lagged: LAGGED.load(Relaxed),
        split,
        lat: stats(&rec),
    }
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// P1: dispatcher task --unbounded--> Python callback thread (recv_timeout).
fn p1(lib: Lib, mode: Mode) -> Run {
    harness(|rt, start| {
        let (tx, rx) = route::<BidAskStkCh>(lib);
        let h = sync_consumer(rx.to_sync(), start);
        let sent = rt.block_on(rt.spawn(pace_task(mode, tx, |s| BidAskStkCh::new(s, 0)))).unwrap();
        (sent, vec![h.join().unwrap()])
    })
}

/// P2: dispatcher task --unbounded--> HTTP server forwarding task
/// (recv().await, to_standard) --tokio broadcast(1000)--> 2 SSE subscriber tasks.
fn p2(lib: Lib, mode: Mode) -> Run {
    harness(|rt, start| {
        let (tx, mut rx) = route::<BidAskStkCh>(lib);
        let (btx, _) = broadcast::channel::<BidAskStk>(1000);
        let subs: Vec<_> = (0..2)
            .map(|_| {
                let mut brx = btx.subscribe();
                rt.spawn(async move {
                    let mut rec = Rec::new(start);
                    loop {
                        match brx.recv().await {
                            Ok(v) => rec.rec(v.t),
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                LAGGED.fetch_add(n, Relaxed);
                            }
                            Err(_) => break,
                        }
                    }
                    rec
                })
            })
            .collect();
        rt.spawn(async move {
            while let Some(v) = rx.recv_async().await {
                let _ = btx.send(BidAskStk::from_t(v.t, v.seq));
            }
        });
        let sent = rt.block_on(rt.spawn(pace_task(mode, tx, |s| BidAskStkCh::new(s, 0)))).unwrap();
        let recs = subs.into_iter().map(|h| rt.block_on(h).unwrap()).collect();
        (sent, recs)
    })
}

/// P3: Solace C callback thread --unbounded--> dispatcher task (recv().await).
fn p3(lib: Lib, mode: Mode) -> Run {
    harness(|rt, start| {
        let (tx, rx) = cthread::<SolMsg>(lib);
        let h = async_consumer(rt, rx, start);
        let sent = thread::spawn(move || pace_sync(mode, tx, |s| SolMsg::new(s, 0))).join().unwrap();
        (sent, vec![rt.block_on(h).unwrap()])
    })
}

/// P4: Solace C callback thread --unbounded--> Python event thread
/// (`get_async_event_receiver().clone_sync()` + recv_timeout).
fn p4(lib: Lib, mode: Mode) -> Run {
    harness(|_rt, start| {
        let (tx, rx) = cthread::<SolEvent>(lib);
        let h = sync_consumer(rx.to_sync(), start);
        let sent = thread::spawn(move || pace_sync(mode, tx, |s| SolEvent::new(s, 0))).join().unwrap();
        (sent, vec![h.join().unwrap()])
    })
}

/// route weights (per 100 messages) for the multi-route fan-out
fn route_of(seq: u64) -> u64 {
    match seq % 100 {
        0..=39 => 0,  // bidask stk
        40..=64 => 1, // tick stk
        65..=79 => 2, // bidask fop
        80..=89 => 3, // tick fop
        90..=93 => 4, // quote stk
        94..=95 => 5, // quote fop
        96..=97 => 6, // quote idx
        _ => 7,       // order event
    }
}

/// P6: C thread --SolMsg--> dispatcher task --8 routes--> 8 Python callback
/// threads. `Crossbeam` here means tokio mpsc for the C hop + crossbeam routes.
fn p6(lib: Lib, mode: Mode) -> Run {
    harness(|rt, start| {
        let clib = if lib == Lib::Crossbeam { Lib::Tokio } else { lib };
        let (ctx, mut crx) = cthread::<SolMsg>(clib);
        let (t0, r0) = route::<BidAskStkCh>(lib);
        let (t1, r1) = route::<TickStkCh>(lib);
        let (t2, r2) = route::<BidAskFopCh>(lib);
        let (t3, r3) = route::<TickFopCh>(lib);
        let (t4, r4) = route::<QuoteStkCh>(lib);
        let (t5, r5) = route::<QuoteFopCh>(lib);
        let (t6, r6) = route::<QuoteIdxCh>(lib);
        let (t7, r7) = route::<OrderEvent>(lib);
        let hs = vec![
            sync_consumer(r0.to_sync(), start),
            sync_consumer(r1.to_sync(), start),
            sync_consumer(r2.to_sync(), start),
            sync_consumer(r3.to_sync(), start),
            sync_consumer(r4.to_sync(), start),
            sync_consumer(r5.to_sync(), start),
            sync_consumer(r6.to_sync(), start),
            sync_consumer(r7.to_sync(), start),
        ];
        rt.spawn(async move {
            while let Some(m) = crx.recv_async().await {
                let (t, s) = (m.t, m.seq);
                match m.pad[0] {
                    0 => t0.send_async(P::from_t(t, s)).await,
                    1 => t1.send_async(P::from_t(t, s)).await,
                    2 => t2.send_async(P::from_t(t, s)).await,
                    3 => t3.send_async(P::from_t(t, s)).await,
                    4 => t4.send_async(P::from_t(t, s)).await,
                    5 => t5.send_async(P::from_t(t, s)).await,
                    6 => t6.send_async(P::from_t(t, s)).await,
                    _ => t7.send_async(P::from_t(t, s)).await,
                };
            }
        });
        let sent = thread::spawn(move || pace_sync(mode, ctx, |s| SolMsg::new(s, route_of(s)))).join().unwrap();
        (sent, hs.into_iter().map(|h| h.join().unwrap()).collect())
    })
}

/// P7: one market-data channel with competing mixed consumers: the Python
/// callback thread (clone_sync + recv_timeout) and a Python asyncio receiver
/// (`get_*_receiver()` -> `recv().await`).
fn p7(lib: Lib, mode: Mode) -> Run {
    harness(|rt, start| {
        let (tx, hs, ha) = match lib {
            Lib::CbBridge => {
                let (s, r) = crossbeam_channel::unbounded::<BidAskStkCh>();
                let (bs, br) = mpsc::unbounded_channel();
                let bridge_rx = r.clone();
                thread::spawn(move || loop {
                    match bridge_rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(v) => {
                            if bs.send(v).is_err() {
                                break;
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(_) => break,
                    }
                });
                (Tx::Cb(s), sync_consumer(Rx::Cb(r), start), async_consumer(rt, Rx::Tk(br), start))
            }
            _ => {
                let (s, r) = route::<BidAskStkCh>(lib);
                let r2 = r.dup();
                (s, sync_consumer(r.to_sync(), start), async_consumer(rt, r2, start))
            }
        };
        let sent = rt.block_on(rt.spawn(pace_task(mode, tx, |s| BidAskStkCh::new(s, 0)))).unwrap();
        let a = hs.join().unwrap();
        let b = rt.block_on(ha).unwrap();
        (sent, vec![a, b])
    })
}

// ---------------------------------------------------------------------------
// Scenario driver
// ---------------------------------------------------------------------------

struct PathDef {
    id: &'static str,
    title: &'static str,
    shape: &'static str,
    variants: Vec<(&'static str, Lib)>,
    run: fn(Lib, Mode) -> Run,
    /// rare-event path: low-rate scenarios instead of long 10k/bursty runs
    low_rate: bool,
    idle: bool,
}

/// label of the plain `kanal` dependency (spin mutex unless this binary was
/// built with the bench feature `std-mutex`)
const KANAL: &str = if cfg!(feature = "std-mutex") { "kanal(std-mutex build)" } else { "kanal-spin" };
const KANAL_STD: &str = "kanal-std-mutex";

fn paths() -> Vec<PathDef> {
    use Lib::*;
    vec![
        PathDef {
            id: "P1",
            title: "market data -> Python callback thread",
            shape: "tokio dispatcher task `send().await` -> unbounded -> OS thread `clone_sync().recv_timeout(100ms)`; ChannelBidAskSTKv1 616 B",
            variants: vec![(KANAL, Kanal), (KANAL_STD, KanalStd), ("crossbeam", Crossbeam), ("flume", Flume), ("async-channel", AsyncChan)],
            run: p1,
            low_rate: false,
            idle: true,
        },
        PathDef {
            id: "P2",
            title: "market data -> HTTP server SSE",
            shape: "dispatcher task -> unbounded -> forwarding task `recv().await` -> to_standard (392 B) -> tokio broadcast(1000) -> 2 SSE subscriber tasks; 616 B",
            variants: vec![(KANAL, Kanal), (KANAL_STD, KanalStd), ("tokio mpsc", Tokio), ("flume", Flume), ("async-channel", AsyncChan)],
            run: p2,
            low_rate: false,
            idle: false,
        },
        PathDef {
            id: "P3",
            title: "Solace C thread -> dispatcher task",
            shape: "rsolace C callback OS thread sync `send` -> unbounded -> tokio dispatcher `recv().await` (msg / p2p channels); SolMsg 24 B",
            variants: vec![(KANAL, Kanal), (KANAL_STD, KanalStd), ("tokio mpsc", Tokio), ("flume", Flume), ("async-channel", AsyncChan)],
            run: p3,
            low_rate: false,
            idle: false,
        },
        PathDef {
            id: "P4",
            title: "Solace session events -> Python event thread",
            shape: "C callback OS thread `send` -> unbounded -> OS thread `as_async().clone_sync().recv_timeout(100ms)`; SolEvent 48 B (rare events)",
            variants: vec![(KANAL, Kanal), (KANAL_STD, KanalStd), ("crossbeam", Crossbeam), ("flume", Flume), ("async-channel", AsyncChan)],
            run: p4,
            low_rate: true,
            idle: true,
        },
        PathDef {
            id: "P6",
            title: "multi-route fan-out (full chain)",
            shape: "C thread SolMsg -> dispatcher task -> 8 unbounded routes (bidask/tick stk+fop, quote stk/fop/idx, order; 392-1104 B, weighted 40/25/15/10/4/2/2/2) -> 8 OS threads recv_timeout(100ms)",
            variants: vec![(KANAL, Kanal), (KANAL_STD, KanalStd), ("crossbeam+tokio", Crossbeam), ("flume", Flume), ("async-channel", AsyncChan)],
            run: p6,
            low_rate: false,
            idle: true,
        },
        PathDef {
            id: "P7",
            title: "MPMC: callback thread + asyncio receiver on one channel",
            shape: "dispatcher task -> one channel -> competing consumers: OS thread recv_timeout(100ms) + tokio task `recv().await`; 616 B",
            variants: vec![
                (KANAL, Kanal),
                (KANAL_STD, KanalStd),
                ("flume", Flume),
                ("async-channel", AsyncChan),
                ("crossbeam+bridge->tokio", CbBridge),
                ("tokio broadcast (all get all)", Broadcast),
            ],
            run: p7,
            low_rate: false,
            idle: true,
        },
    ]
}

fn group_paths(group: &str) -> Vec<&'static str> {
    match group {
        "A1" => vec!["P1", "P4"],
        "A2" => vec!["P7"],
        "B" => vec!["P2", "P3"],
        "C" => vec!["P6"],
        _ => vec!["P1", "P2", "P3", "P4", "P6", "P7"],
    }
}

#[derive(Serialize, Clone, Default)]
struct VariantResult {
    label: String,
    thr_mps: Vec<f64>,
    thr_median: f64,
    paced_1k: Vec<Run>,
    paced_50k: Vec<Run>,
    paced_100: Option<Run>,
    long_10k: Option<Run>,
    bursty: Option<Run>,
    idle: Option<Run>,
}

#[derive(Serialize)]
struct PathResult {
    id: String,
    title: String,
    shape: String,
    variants: Vec<VariantResult>,
}

#[derive(Serialize)]
struct Report {
    os: String,
    arch: String,
    cores: usize,
    group: String,
    contended: bool,
    burner_pids: Vec<u32>,
    long_secs: f64,
    paths: Vec<PathResult>,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// the repeat whose p99 is the median
fn median_run(v: &[Run]) -> &Run {
    let mut idx: Vec<usize> = (0..v.len()).collect();
    idx.sort_by(|&a, &b| v[a].lat.p99.partial_cmp(&v[b].lat.p99).unwrap());
    &v[idx[idx.len() / 2]]
}

struct Cfg {
    long_secs: f64,
    short_secs: f64,
    reps: usize,
    flood_n: u64,
    idle_secs: f64,
}

fn run_path(p: &PathDef, cfg: &Cfg) -> PathResult {
    let mut res: Vec<VariantResult> = p
        .variants
        .iter()
        .map(|(l, _)| VariantResult { label: l.to_string(), ..Default::default() })
        .collect();
    let log = |what: &str, label: &str, r: &Run| {
        eprintln!(
            "  {} {:<30} {:<12} n={:<8} mps={:>10.0} p50={:>8.1} p99={:>8.1} p99.9={:>8.1} max={:>9.1} cpu%={:>5.1} lag={}",
            p.id, label, what, r.delivered, r.mps, r.lat.p50, r.lat.p99, r.lat.p999, r.lat.max, r.cpu_pct, r.lagged
        )
    };
    // interleave variants within each scenario to spread runner drift
    for _ in 0..cfg.reps {
        for (i, (label, lib)) in p.variants.iter().enumerate() {
            let r = (p.run)(*lib, Mode::Flood { n: cfg.flood_n });
            log("flood", label, &r);
            res[i].thr_mps.push(r.mps);
        }
    }
    for _ in 0..cfg.reps {
        for (i, (label, lib)) in p.variants.iter().enumerate() {
            let r = (p.run)(*lib, Mode::Paced { rate: 1000, secs: cfg.short_secs });
            log("1k", label, &r);
            res[i].paced_1k.push(r);
            if !p.low_rate {
                let r = (p.run)(*lib, Mode::Paced { rate: 50_000, secs: cfg.short_secs });
                log("50k", label, &r);
                res[i].paced_50k.push(r);
            }
        }
    }
    for (i, (label, lib)) in p.variants.iter().enumerate() {
        if p.low_rate {
            let r = (p.run)(*lib, Mode::Paced { rate: 100, secs: cfg.short_secs * 3.0 });
            log("100/s", label, &r);
            res[i].paced_100 = Some(r);
        } else {
            let r = (p.run)(*lib, Mode::Paced { rate: 10_000, secs: cfg.long_secs });
            log("10k-long", label, &r);
            res[i].long_10k = Some(r);
            let r = (p.run)(*lib, Mode::Bursty { burst: 5000, every_ms: 100, secs: cfg.long_secs / 2.0 });
            log("bursty", label, &r);
            res[i].bursty = Some(r);
        }
        if p.idle {
            let r = (p.run)(*lib, Mode::Idle { secs: cfg.idle_secs });
            log("idle", label, &r);
            res[i].idle = Some(r);
        }
    }
    for v in &mut res {
        v.thr_median = median(v.thr_mps.clone());
    }
    PathResult { id: p.id.into(), title: p.title.into(), shape: p.shape.into(), variants: res }
}

// ---------------------------------------------------------------------------
// Markdown
// ---------------------------------------------------------------------------

fn f(x: f64) -> String {
    if x >= 1000.0 {
        format!("{x:.0}")
    } else if x >= 10.0 {
        format!("{x:.1}")
    } else {
        format!("{x:.2}")
    }
}

fn ratio(x: f64, base: f64) -> String {
    if base > 0.0 {
        format!("{:.2}", x / base)
    } else {
        "-".into()
    }
}

fn stab_table(out: &mut String, title: &str, rows: &[(&str, &Run)]) {
    out.push_str(&format!("\n{title}\n\n"));
    out.push_str("| variant | p50 µs | p99 | p99.9 | p99.99 | max | stdev | p99.9/p50 | >100µs | >1ms | >10ms | secs max>1ms | p99.9 ×kanal | max ×kanal | lagged |\n");
    out.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    let base = rows[0].1;
    for (label, r) in rows {
        let s = &r.lat;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {:.1} | {} | {} | {} | {}/{} | {} | {} | {} |\n",
            label,
            f(s.p50),
            f(s.p99),
            f(s.p999),
            f(s.p9999),
            f(s.max),
            f(s.stdev),
            if s.p50 > 0.0 { s.p999 / s.p50 } else { 0.0 },
            s.over_100us,
            s.over_1ms,
            s.over_10ms,
            s.secs_max_over_1ms,
            s.secs,
            ratio(s.p999, base.lat.p999),
            ratio(s.max, base.lat.max),
            r.lagged
        ));
    }
    out.push_str("\nTop spikes (µs @ send offset s):\n\n");
    for (label, r) in rows {
        let t: Vec<String> = r.lat.top.iter().map(|(l, a)| format!("{}@{:.2}", f(*l), a)).collect();
        let mut sm = r.lat.sec_max_us.clone();
        let med = if sm.is_empty() { 0.0 } else { median(std::mem::take(&mut sm)) };
        out.push_str(&format!("- {label}: {} (per-second max: median {} µs)\n", t.join(", "), f(med)));
    }
}

fn markdown(rep: &Report) -> String {
    let mut o = String::new();
    o.push_str(&format!(
        "## {} / {} — {} logical cores — group {} — {}\n\n",
        rep.os,
        rep.arch,
        rep.cores,
        rep.group,
        if rep.contended {
            format!("CPU contention: {} busy-loop processes (PIDs {:?})", rep.burner_pids.len(), rep.burner_pids)
        } else {
            "no contention".into()
        }
    ));
    o.push_str(&format!("Latency = send Instant -> consumer receive, µs. Long paced run {:.0} s at 10k msg/s; bursty = 5000-msg bursts every 100 ms for {:.0} s.\n", rep.long_secs, rep.long_secs / 2.0));
    for p in &rep.paths {
        o.push_str(&format!("\n### {} {}\n\n{}\n\n", p.id, p.title, p.shape));
        let base = &p.variants[0];
        o.push_str("| variant | flood M msg/s | ×kanal | 1k/s p50 / p99 / max µs | 50k/s p50 / p99 / max µs | CPU µs/msg @10k | idle CPU % | delivered/sent @10k | split |\n");
        o.push_str("|---|---|---|---|---|---|---|---|---|\n");
        for v in &p.variants {
            let a = median_run(&v.paced_1k);
            let b = if v.paced_50k.is_empty() {
                "-".to_string()
            } else {
                let b = median_run(&v.paced_50k);
                format!("{} / {} / {}", f(b.lat.p50), f(b.lat.p99), f(b.lat.max))
            };
            let long = v.long_10k.as_ref().or(v.paced_100.as_ref()).unwrap();
            o.push_str(&format!(
                "| {} | {:.2} | {} | {} / {} / {} | {} | {} | {} | {:.2} | {:?} |\n",
                v.label,
                v.thr_median / 1e6,
                ratio(v.thr_median, base.thr_median),
                f(a.lat.p50),
                f(a.lat.p99),
                f(a.lat.max),
                b,
                f(long.cpu_ns_per_msg / 1000.0),
                v.idle.as_ref().map(|r| format!("{:.2}", r.cpu_pct)).unwrap_or("-".into()),
                long.delivered as f64 / long.sent.max(1) as f64,
                long.split
            ));
        }
        if p.variants[0].long_10k.is_some() {
            let rows: Vec<(&str, &Run)> = p.variants.iter().map(|v| (v.label.as_str(), v.long_10k.as_ref().unwrap())).collect();
            stab_table(&mut o, &format!("Stability, paced 10k msg/s for {:.0} s:", rep.long_secs), &rows);
            let rows: Vec<(&str, &Run)> = p.variants.iter().map(|v| (v.label.as_str(), v.bursty.as_ref().unwrap())).collect();
            stab_table(&mut o, "Stability, bursty open (5000 msgs every 100 ms):", &rows);
        } else {
            let rows: Vec<(&str, &Run)> = p.variants.iter().map(|v| (v.label.as_str(), v.paced_100.as_ref().unwrap())).collect();
            stab_table(&mut o, "Stability, 100 msg/s (consumer parks between events):", &rows);
        }
    }
    o
}

// ---------------------------------------------------------------------------
// CPU contention: child busy-loop processes (own PIDs, killed afterwards)
// ---------------------------------------------------------------------------

fn burn() -> ! {
    // exit as soon as the parent goes away (stdin pipe closes)
    thread::spawn(|| {
        let mut b = [0u8; 1];
        let _ = std::io::stdin().read(&mut b);
        std::process::exit(0);
    });
    let mut x = 1u64;
    loop {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        black_box(x);
    }
}

struct Burners(Vec<Child>);
impl Drop for Burners {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--burn") {
        burn();
    }
    let get = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned();
    let group = get("--group").unwrap_or_else(|| "all".into());
    let contend = args.iter().any(|a| a == "--contend");
    let quick = args.iter().any(|a| a == "--quick");
    let out_dir = get("--out").unwrap_or_else(|| "results".into());
    let long_secs: f64 = get("--long-secs").map(|s| s.parse().unwrap()).unwrap_or(if quick { 4.0 } else { 60.0 });
    let cfg = Cfg {
        long_secs,
        short_secs: if quick { 1.0 } else { 3.0 },
        reps: if quick { 1 } else { 3 },
        flood_n: if quick { 50_000 } else { 200_000 },
        idle_secs: if quick { 1.0 } else { 5.0 },
    };
    now_ns();
    let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    let burners = if contend {
        let exe = std::env::current_exe().unwrap();
        Burners(
            (0..cores)
                .map(|_| Command::new(&exe).arg("--burn").stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap())
                .collect(),
        )
    } else {
        Burners(Vec::new())
    };
    let burner_pids: Vec<u32> = burners.0.iter().map(|c| c.id()).collect();
    if contend {
        eprintln!("started {} burner processes: {:?}", burner_pids.len(), burner_pids);
    }

    // --kanal-only: only kanal-spin vs kanal-std-mutex; --reverse: run the
    // variants in reverse order (alternate the order between repetitions)
    let kanal_only = args.iter().any(|a| a == "--kanal-only");
    let reverse = args.iter().any(|a| a == "--reverse");
    let mut defs = paths();
    for p in &mut defs {
        if kanal_only {
            p.variants.retain(|(_, l)| matches!(l, Lib::Kanal | Lib::KanalStd));
        }
        if reverse {
            p.variants.reverse();
        }
    }
    let wanted = group_paths(&group);
    let mut results = Vec::new();
    for p in defs.iter().filter(|p| wanted.contains(&p.id)) {
        eprintln!("== {} {}", p.id, p.title);
        results.push(run_path(p, &cfg));
    }
    drop(burners);
    if contend {
        eprintln!("stopped burner processes {:?}", burner_pids);
    }

    let rep = Report {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        cores,
        group: group.clone(),
        contended: contend,
        burner_pids,
        long_secs,
        paths: results,
    };
    let mut tag = format!("{}-{}-{}", rep.os, group, if contend { "contended" } else { "normal" });
    if let Some(t) = get("--tag") {
        tag = format!("{tag}-{t}");
    }
    std::fs::create_dir_all(&out_dir).unwrap();
    let md = markdown(&rep);
    std::fs::write(format!("{out_dir}/{tag}.md"), &md).unwrap();
    std::fs::write(format!("{out_dir}/{tag}.json"), serde_json::to_string(&rep).unwrap()).unwrap();
    println!("{md}");
}
