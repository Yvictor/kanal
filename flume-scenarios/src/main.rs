//! Runner. `flume-scenarios short|full [--contend]`, `soak --secs N --size 24|500|1100
//! [--contend] [--csv PATH]`, `repro`, `child s5a|s5b`.

use flume_scenarios::common::{Large, Medium, Outcome, Small};
use flume_scenarios::{repro, s1, s2, s3, s4, s5, soak};
use std::time::Duration;

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter().position(|a| a == k).and_then(|i| args.get(i + 1).cloned())
}

/// Run S5 cases in a child process so an abort (double panic) cannot take the runner down.
fn child(case: &str) -> Outcome {
    let exe = std::env::current_exe().unwrap();
    let out = std::process::Command::new(exe).args(["child", case]).output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    if out.status.success() {
        let line = stdout.lines().find(|l| l.starts_with("| S5")).unwrap_or("").to_string();
        let parts: Vec<&str> = line.trim_matches('|').splitn(3, '|').collect();
        if parts.len() == 3 {
            let mut o = Outcome::new(parts[0].trim(), !parts[1].contains("FAIL"), parts[2].trim());
            o.status = parts[1].trim().to_string();
            return o;
        }
    }
    Outcome::new(
        format!("S5 child {case}"),
        false,
        format!("child exited {:?}: {}", out.status, String::from_utf8_lossy(&out.stderr).lines().last().unwrap_or("")),
    )
}

fn suite(full: bool, contend: bool, soak_secs: u64) -> Vec<Outcome> {
    let mut v = vec![];
    let mut push = |o: Outcome| {
        o.print();
        v.push(o);
    };
    let k = if full { 5 } else { 1 };
    // S1
    let (s1secs, chans) = if full { (30.0, 8) } else { (8.0, 4) };
    push(s1::run::<0>(s1secs, chans, contend));
    push(s1::run::<476>(s1secs, chans, contend));
    push(s1::run::<1076>(s1secs, chans, contend));
    // S2
    push(s2::run::<0>(100_000 * k, None, contend));
    push(s2::run::<476>(50_000 * k, None, contend));
    push(s2::run::<1076>(50_000 * k, None, contend));
    push(s2::run::<476>(50_000 * k, Some(65536), contend));
    push(s2::run::<0>(50_000 * k, Some(64), contend));
    // S3
    push(s3::states_recv());
    push(s3::states_send());
    push(s3::recv_stress::<0>(100_000 * k, contend));
    push(s3::recv_stress::<476>(50_000 * k, contend));
    push(s3::recv_stress::<1076>(50_000 * k, contend));
    push(s3::send_stress::<0>(20_000 * k, contend));
    push(s3::send_stress::<1076>(10_000 * k, contend));
    // S4
    push(s4::last_sender::<0>(300 * k, contend));
    push(s4::last_sender::<1076>(150 * k, contend));
    push(s4::last_receiver::<476>(200 * k, contend));
    push(s4::weak_sender(200 * k));
    // S5 (isolated)
    push(child("s5a"));
    push(child("s5b"));
    // Known-defect reproductions
    push(repro::orphaned_recv_timeout(Duration::from_secs(2), Duration::from_millis(100)));
    push(repro::orphaned_no_cancel(Duration::from_secs(2), Duration::from_millis(100)));
    push(repro::orphaned_recv(Duration::from_millis(300)));
    push(repro::orphaned_realistic(if full { 100 } else { 20 }, true));
    push(repro::orphaned_realistic(if full { 100 } else { 20 }, false));
    push(repro::sticky_woken(100_000));
    // S6 (shortened unless soak_secs is large)
    if soak_secs > 0 {
        push(soak::run::<476>(soak_secs, contend, Some(format!("results/soak-500-{}.csv", if contend { "contended" } else { "normal" }).into())));
        push(soak::run::<0>(soak_secs / 3 + 1, contend, None));
        push(soak::run::<1076>(soak_secs / 3 + 1, contend, None));
    }
    v
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("short");
    let contend = args.iter().any(|a| a == "--contend");
    let header = || {
        println!("| scenario | result | detail |\n|---|---|---|");
    };
    let results: Vec<Outcome> = match cmd {
        "child" => {
            let o = match args.get(2).map(|s| s.as_str()) {
                Some("s5a") => s5::payload_drop(),
                Some("s5b") => s5::panicking_waker(),
                _ => panic!("unknown child"),
            };
            o.print();
            std::process::exit(if o.ok { 0 } else { 1 });
        }
        "short" | "full" => {
            println!(
                "### flume 0.12.0 scenarios: {cmd}{} on {} ({} cpus)\n",
                if contend { " + CPU contention" } else { "" },
                std::env::consts::OS,
                std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
            );
            header();
            let soak_secs = arg(&args, "--soak-secs").map(|s| s.parse().unwrap()).unwrap_or(30);
            suite(cmd == "full", contend, soak_secs)
        }
        "soak" => {
            header();
            let secs = arg(&args, "--secs").map(|s| s.parse().unwrap()).unwrap_or(600);
            let csv = arg(&args, "--csv").map(Into::into);
            let o = match arg(&args, "--size").as_deref().unwrap_or("500") {
                "24" => soak::run::<0>(secs, contend, csv),
                "1100" => soak::run::<1076>(secs, contend, csv),
                _ => soak::run::<476>(secs, contend, csv),
            };
            o.print();
            vec![o]
        }
        "repro" => {
            header();
            let v = vec![
                repro::orphaned_recv_timeout(Duration::from_secs(2), Duration::from_millis(100)),
                repro::orphaned_no_cancel(Duration::from_secs(2), Duration::from_millis(100)),
                repro::orphaned_recv(Duration::from_millis(300)),
                repro::orphaned_realistic(50, true),
                repro::orphaned_realistic(50, false),
                repro::sticky_woken(100_000),
            ];
            v.iter().for_each(|o| o.print());
            v
        }
        _ => panic!("unknown command {cmd}"),
    };
    let _ = (Small::new, Medium::new, Large::new);
    let failed = results.iter().filter(|o| !o.ok).count();
    println!("\n{} scenarios, {} failed", results.len(), failed);
    std::process::exit(if failed > 0 { 1 } else { 0 });
}
