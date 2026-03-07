use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
struct Row {
    group: String,
    conc: usize,
    _ops: u64,
    _duration_ns: u128,
    ops_per_sec: f64,
}

fn report_path() -> PathBuf {
    if let Ok(p) = std::env::var("IGNITE_BENCH_REPORT") {
        return PathBuf::from(p);
    }
    let mut p = PathBuf::from("target/criterion");
    p.push("ignite_summary.csv");
    p
}

fn agg_path() -> PathBuf {
    if let Ok(p) = std::env::var("IGNITE_BENCH_REPORT_AGG") {
        return PathBuf::from(p);
    }
    let mut p = PathBuf::from("target/criterion");
    p.push("ignite_summary_agg.csv");
    p
}

fn parse_csv(path: &PathBuf) -> std::io::Result<Vec<Row>> {
    let f = File::open(path)?;
    let rdr = BufReader::new(f);
    let mut rows = Vec::new();
    for (i, line_res) in rdr.lines().enumerate() {
        let line = line_res?;
        if i == 0 {
            continue; // header
        }
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 5 {
            continue;
        }
        let group = parts[0].to_string();
        let conc: usize = parts[1].parse().unwrap_or(0);
        let ops: u64 = parts[2].parse().unwrap_or(0);
        let duration_ns: u128 = parts[3].parse().unwrap_or(0);
        let ops_per_sec: f64 = parts[4].parse().unwrap_or(0.0);
        rows.push(Row {
            group,
            conc,
            _ops: ops,
            _duration_ns: duration_ns,
            ops_per_sec,
        });
    }
    Ok(rows)
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn main() -> std::io::Result<()> {
    let path = report_path();
    let rows = parse_csv(&path)?;
    if rows.is_empty() {
        eprintln!("No rows found in {:?}", path);
        return Ok(());
    }

    // group -> conc -> vec of ops/sec
    let mut map: BTreeMap<String, BTreeMap<usize, Vec<f64>>> = BTreeMap::new();
    for r in rows {
        map.entry(r.group)
            .or_default()
            .entry(r.conc)
            .or_default()
            .push(r.ops_per_sec);
    }

    let agg_path = agg_path();
    if let Some(dir) = agg_path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&agg_path)?;
    writeln!(
        out,
        "group,concurrency,samples,mean_ops_per_sec,median_ops_per_sec,p90_ops_per_sec,p95_ops_per_sec"
    )?;

    println!("Aggregate ops/sec (mean/median/p90/p95) by group and concurrency:\n");
    for (group, conc_map) in &mut map {
        for (conc, vals) in conc_map {
            let samples = vals.len();
            let mean = if samples > 0 {
                vals.iter().copied().sum::<f64>() / samples as f64
            } else {
                0.0
            };
            let med = {
                let mut tmp = vals.clone();
                median(&mut tmp)
            };
            let p90 = {
                let mut tmp = vals.clone();
                percentile(&mut tmp, 0.90)
            };
            let p95 = {
                let mut tmp = vals.clone();
                percentile(&mut tmp, 0.95)
            };
            println!(
                "- {} @ {}: mean = {:.3} ops/s, median = {:.3} ops/s, p90 = {:.3}, p95 = {:.3} (n={})",
                group, conc, mean, med, p90, p95, samples
            );
            writeln!(
                out,
                "{},{},{},{:.6},{:.6},{:.6},{:.6}",
                group, conc, samples, mean, med, p90, p95
            )?;
        }
    }

    println!("\nWrote {}", agg_path.display());

    // Comparative conclusion: async vs sync for matching groups
    println!("\n=== Comparison: async vs sync (median ops/sec ratios) ===\n");
    // Build aggregated medians for convenience
    let mut med_map: BTreeMap<String, BTreeMap<usize, f64>> = BTreeMap::new();
    for (group, conc_map) in &map {
        let mut cm = BTreeMap::new();
        for (conc, vals) in conc_map {
            let mut tmp = vals.clone();
            cm.insert(*conc, median(&mut tmp));
        }
        med_map.insert(group.clone(), cm);
    }

    // helper to normalize group names to (base, kind)
    fn norm(name: &str) -> Option<(String, &'static str)> {
        if let Some(idx) = name.find("_sync_bytes_") {
            let base = format!("{}{}_{}", &name[..idx], "_bytes", &name[idx + 12..]);
            return Some((base, "sync"));
        }
        if let Some(idx) = name.find("_async_bytes_") {
            let base = format!("{}{}_{}", &name[..idx], "_bytes", &name[idx + 13..]);
            return Some((base, "async"));
        }
        if let Some(base) = name.strip_suffix("_sync") {
            return Some((base.to_string(), "sync"));
        }
        if let Some(base) = name.strip_suffix("_async") {
            return Some((base.to_string(), "async"));
        }
        None
    }

    // Build paired groups per base
    let mut bases: BTreeMap<String, (Option<BTreeMap<usize, f64>>, Option<BTreeMap<usize, f64>>)> =
        BTreeMap::new();
    for (g, cm) in &med_map {
        if let Some((base, kind)) = norm(g) {
            let entry = bases.entry(base).or_insert((None, None));
            match kind {
                "sync" => entry.0 = Some(cm.clone()),
                "async" => entry.1 = Some(cm.clone()),
                _ => {}
            }
        }
    }

    for (base, (sync_opt, async_opt)) in bases {
        if let (Some(sync_map), Some(async_map)) = (sync_opt, async_opt) {
            // For shared conc keys, compute ratio async/sync
            let mut ratios = Vec::new();
            println!("Group '{}':", base);
            for (conc, sync_med) in &sync_map {
                if let Some(async_med) = async_map.get(conc) {
                    let ratio = if *sync_med > 0.0 {
                        async_med / sync_med
                    } else {
                        0.0
                    };
                    ratios.push(ratio);
                    println!(
                        "  - conc {:>2}: sync med = {:>12.3} ops/s, async med = {:>12.3} ops/s, async/sync = {:>6.3}x",
                        conc, sync_med, async_med, ratio
                    );
                }
            }
            if !ratios.is_empty() {
                let mean = ratios.iter().copied().sum::<f64>() / ratios.len() as f64;
                let mut tmp = ratios.clone();
                let med = median(&mut tmp);
                let conclusion = if med > 1.05 {
                    format!(
                        "Async tends to be {:.1}% faster (median ratio {:.3}x)",
                        (med - 1.0) * 100.0,
                        med
                    )
                } else if med < 0.95 {
                    format!(
                        "Async tends to be {:.1}% slower (median ratio {:.3}x)",
                        (1.0 - med) * 100.0,
                        med
                    )
                } else {
                    format!("Async and sync are comparable (median ratio {:.3}x)", med)
                };
                println!(
                    "  -> Summary: mean ratio = {:.3}x, median ratio = {:.3}x. {}\n",
                    mean, med, conclusion
                );
            }
        }
    }
    Ok(())
}

fn percentile(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    // Nearest-rank method
    let rank = (p * (n as f64)).ceil() as usize;
    let idx = if rank == 0 { 0 } else { rank - 1 };
    v[idx]
}
