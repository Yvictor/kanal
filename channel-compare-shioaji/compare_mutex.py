#!/usr/bin/env python3
"""Compare kanal lock variants against kanal-spin from channel-compare-shioaji
JSON results (run with --kanal-only). Usage: compare_mutex.py DIR [DIR...]

Variants: spin (baseline), std = std-mutex, ns = nosleep spin, pl = parking_lot.
Repetitions of the same os/group/load are pooled: latency percentiles and
CPU are averaged, max is the worst, threshold counts and seconds>1ms are
summed, throughput is the mean of the per-rep flood medians.
Flags (candidate vs spin): p50 > +10% and > +1 µs, p99.9 > +25% and > +20 µs,
max > +25% and > +500 µs, >1ms count > +5%, secs>1ms higher,
throughput < 0.9x, CPU/msg > +15% and > +0.5 µs.
"""
import json
import pathlib
import statistics
import sys
from collections import defaultdict

BASE = "kanal-spin"
CANDS = [("kanal-std-mutex", "std"), ("kanal-nosleep", "ns"), ("kanal-parking-lot", "pl")]
SCEN = [("long_10k", "10k/s"), ("bursty", "bursty"), ("paced_100", "100/s")]


def load(dirs):
    runs = defaultdict(list)
    thr = defaultdict(lambda: defaultdict(list))
    idle = defaultdict(lambda: defaultdict(list))
    files = 0
    for d in dirs:
        for f in sorted(pathlib.Path(d).rglob("*.json")):
            if f.name.startswith("compare"):
                continue
            rep = json.loads(f.read_text())
            if "paths" not in rep:
                continue
            files += 1
            ld = "contended" if rep["contended"] else "normal"
            for p in rep["paths"]:
                vs = {v["label"]: v for v in p["variants"]}
                if BASE not in vs:
                    continue
                key = (rep["os"], ld, p["id"])
                for lab, v in vs.items():
                    thr[key][lab].append(v["thr_median"])
                    if v.get("idle"):
                        idle[key][lab].append(v["idle"]["cpu_pct"])
                for sk, _ in SCEN:
                    if vs[BASE].get(sk):
                        runs[key + (sk,)].append({lab: v[sk] for lab, v in vs.items()})
    return runs, thr, idle, files


def agg(rs):
    lat = [r["lat"] for r in rs]
    m = lambda k: statistics.mean(x[k] for x in lat)
    return {
        "n": len(rs),
        "p50": m("p50"),
        "p99": m("p99"),
        "p999": m("p999"),
        "max": max(x["max"] for x in lat),
        "o1": sum(x["over_1ms"] for x in lat),
        "o10": sum(x["over_10ms"] for x in lat),
        "s1": sum(x["secs_max_over_1ms"] for x in lat),
        "secs": sum(x["secs"] for x in lat),
        "cpu": statistics.mean(r["cpu_ns_per_msg"] for r in rs) / 1000.0,
        "reps_p50": [x["p50"] for x in lat],
        "reps_p999": [x["p999"] for x in lat],
        "reps_max": [x["max"] for x in lat],
        "reps_o1": [x["over_1ms"] for x in lat],
    }


def flags(a, b, ta, tb):
    f = []
    if b["p50"] > a["p50"] * 1.10 and b["p50"] - a["p50"] > 1:
        f.append("p50")
    if b["p999"] > a["p999"] * 1.25 and b["p999"] - a["p999"] > 20:
        f.append("p99.9")
    if b["max"] > a["max"] * 1.25 and b["max"] - a["max"] > 500:
        f.append("max")
    if b["o1"] > a["o1"] * 1.05:
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
    present = [(l, s) for l, s in CANDS if any(l in r for rs in runs.values() for r in rs)]
    labs = [BASE] + [l for l, _ in present]
    hdr = "spin/" + "/".join(s for _, s in present)
    out = [f"# kanal lock variants vs spin ({files} result files)\n", __doc__.split("\n\n", 1)[1], ""]
    summary = []
    for os_ in sorted({k[0] for k in runs}):
        out.append(f"\n## {os_}\n\nvalues are {hdr}\n")
        out.append("| load | path | scen | reps | p50 µs | p99 | p99.9 | max | >1ms | >10ms | secs>1ms | CPU µs/msg | flood Mmsg/s | idle CPU% | "
                   + " | ".join(f"{s} worse" for _, s in present) + " |")
        out.append("|" + "---|" * (14 + len(present)))
        for k in sorted(k for k in runs if k[0] == os_):
            _, ld, path, sk = k
            A = {l: agg([r[l] for r in runs[k] if l in r]) for l in labs}
            t = thr[(os_, ld, path)]
            T = {l: statistics.mean(t[l]) / 1e6 for l in labs}
            i = idle[(os_, ld, path)]
            idl = "/".join(f"{statistics.mean(i[l]):.2f}" for l in labs) if i.get(BASE) else "-"
            j = lambda key, fmt=fm: "/".join(fmt(A[l][key]) for l in labs)
            fl = {s: flags(A[BASE], A[l], T[BASE], T[l]) for l, s in present}
            out.append(
                f"| {ld} | {path} | {dict(SCEN)[sk]} | {A[BASE]['n']} | {j('p50')} | {j('p99')} | {j('p999')} | {j('max')} | "
                f"{j('o1', str)} | {j('o10', str)} | {j('s1', str)} (of {A[BASE]['secs']}) | {j('cpu', lambda x: f'{x:.2f}')} | "
                + "/".join(f"{T[l]:.2f}" for l in labs)
                + f" | {idl} | "
                + " | ".join(" ".join(fl[s]) or "no" for _, s in present)
                + " |"
            )
            summary.append({"os": os_, "load": ld, "path": path, "scen": dict(SCEN)[sk],
                            "agg": A, "thr": T, "flags": fl,
                            "thr_reps": {l: [x / 1e6 for x in t[l]] for l in labs}})
    print("\n".join(out))
    pathlib.Path("compare_summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
