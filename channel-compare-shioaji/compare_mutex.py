#!/usr/bin/env python3
"""Compare kanal-spin vs kanal-std-mutex from channel-compare-shioaji JSON
results (run with --kanal-only). Usage: compare_mutex.py DIR [DIR...]

Repetitions of the same os/group/load are pooled: latency percentiles and
CPU are averaged, max is the worst, threshold counts and seconds>1ms are
summed, throughput is the mean of the per-rep flood medians.
Flags (std vs spin): p50 > +10% and > +1 µs, p99.9 > +25% and > +20 µs,
max > +25% and > +500 µs, >1ms count higher, secs>1ms higher, throughput
< 0.9x, CPU/msg > +15%.
"""
import json
import pathlib
import statistics
import sys
from collections import defaultdict

SPIN, STD = "kanal-spin", "kanal-std-mutex"
SCEN = [("long_10k", "10k/s"), ("bursty", "bursty"), ("paced_100", "100/s")]


def load(dirs):
    runs = defaultdict(list)  # (os, load, path, scen) -> [(variant-> dict)]
    thr = defaultdict(lambda: defaultdict(list))  # (os, load, path) -> variant -> [mps]
    idle = defaultdict(lambda: defaultdict(list))
    files = 0
    for d in dirs:
        for f in sorted(pathlib.Path(d).rglob("*.json")):
            rep = json.loads(f.read_text())
            if "paths" not in rep:
                continue
            files += 1
            ld = "contended" if rep["contended"] else "normal"
            for p in rep["paths"]:
                vs = {v["label"]: v for v in p["variants"]}
                if SPIN not in vs or STD not in vs:
                    continue
                key = (rep["os"], ld, p["id"])
                for lab, v in vs.items():
                    thr[key][lab].append(v["thr_median"])
                    if v.get("idle"):
                        idle[key][lab].append(v["idle"]["cpu_pct"])
                for sk, _ in SCEN:
                    if vs[SPIN].get(sk):
                        runs[key + (sk,)].append({lab: vs[lab][sk] for lab in (SPIN, STD)})
    return runs, thr, idle, files


def agg(rs):
    lat = [r["lat"] for r in rs]
    m = lambda k: statistics.mean(x[k] for x in lat)
    return {
        "n": len(rs),
        "p50": m("p50"),
        "p99": m("p99"),
        "p999": m("p999"),
        "p9999": m("p9999"),
        "max": max(x["max"] for x in lat),
        "stdev": m("stdev"),
        "o100": sum(x["over_100us"] for x in lat),
        "o1": sum(x["over_1ms"] for x in lat),
        "o10": sum(x["over_10ms"] for x in lat),
        "s1": sum(x["secs_max_over_1ms"] for x in lat),
        "secs": sum(x["secs"] for x in lat),
        "cpu": statistics.mean(r["cpu_ns_per_msg"] for r in rs) / 1000.0,
    }


def flags(a, b, ta, tb):
    f = []
    if b["p50"] > a["p50"] * 1.10 and b["p50"] - a["p50"] > 1:
        f.append("p50")
    if b["p999"] > a["p999"] * 1.25 and b["p999"] - a["p999"] > 20:
        f.append("p99.9")
    if b["max"] > a["max"] * 1.25 and b["max"] - a["max"] > 500:
        f.append("max")
    if b["o1"] > a["o1"]:
        f.append(">1ms")
    if b["s1"] > a["s1"]:
        f.append("s>1ms")
    if b["cpu"] > a["cpu"] * 1.15 and b["cpu"] - a["cpu"] > 0.5:
        f.append("cpu")
    if tb < ta * 0.9:
        f.append("thr")
    return f


def fm(x):
    return f"{x:.0f}" if x >= 1000 else (f"{x:.1f}" if x >= 10 else f"{x:.2f}")


def main():
    runs, thr, idle, files = load(sys.argv[1:] or ["results"])
    out = [f"# kanal spin vs std-mutex ({files} result files)\n", __doc__.split("\n\n", 1)[1], ""]
    summary = []
    for os_ in sorted({k[0] for k in runs}):
        out.append(f"\n## {os_}\n")
        out.append("| load | path | scen | reps | p50 spin/std µs | p99 | p99.9 | p99.99 | max | >100µs | >1ms | >10ms | secs>1ms | CPU µs/msg | flood Mmsg/s | idle CPU% | std worse |")
        out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for k in sorted(k for k in runs if k[0] == os_):
            _, ld, path, sk = k
            a = agg([r[SPIN] for r in runs[k]])
            b = agg([r[STD] for r in runs[k]])
            t = thr[(os_, ld, path)]
            ta, tb = statistics.mean(t[SPIN]) / 1e6, statistics.mean(t[STD]) / 1e6
            i = idle[(os_, ld, path)]
            idl = f"{statistics.mean(i[SPIN]):.2f}/{statistics.mean(i[STD]):.2f}" if i.get(SPIN) else "-"
            fl = flags(a, b, ta, tb)
            sn = dict(SCEN)[sk]
            out.append(
                f"| {ld} | {path} | {sn} | {a['n']} | {fm(a['p50'])}/{fm(b['p50'])} | {fm(a['p99'])}/{fm(b['p99'])} | "
                f"{fm(a['p999'])}/{fm(b['p999'])} | {fm(a['p9999'])}/{fm(b['p9999'])} | {fm(a['max'])}/{fm(b['max'])} | "
                f"{a['o100']}/{b['o100']} | {a['o1']}/{b['o1']} | {a['o10']}/{b['o10']} | {a['s1']}/{b['s1']} (of {a['secs']}) | "
                f"{a['cpu']:.2f}/{b['cpu']:.2f} | {ta:.2f}/{tb:.2f} | {idl} | {' '.join(fl) or 'no'} |"
            )
            summary.append({"os": os_, "load": ld, "path": path, "scen": sn, "spin": a, "std": b,
                            "thr_spin": ta, "thr_std": tb, "flags": fl})
    print("\n".join(out))
    pathlib.Path("compare_summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
