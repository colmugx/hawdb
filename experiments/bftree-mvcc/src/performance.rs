// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::adapter::{
    Error, ReadTxn, Result, Store, MAX_GC_WORK, MAX_SCAN_RECORDS, MAX_SCAN_ROWS, MAX_WRITES,
};
use crate::benchmark::{nanos, value, KeyShape, Latency};
use crate::index::Layout;
use hawdb_qos::ProcessMemorySnapshot;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

// The manual structural driver replaces only this hook in its copied harness.
// Diagnostic counters are never used for qualified timing comparisons.
fn diagnostic(_phase: &str) {}

#[derive(Serialize)]
struct BatchTiming {
    shape: &'static str,
    width: usize,
    samples: usize,
    scalar: Latency,
    batch: Latency,
    cursor_starts: usize,
    cursor_resolutions: usize,
    scanned_records: usize,
    point_probes: usize,
}

#[derive(Serialize)]
struct ReadTiming {
    point: Latency,
    scan: Latency,
    batches: Vec<BatchTiming>,
}

fn read_phase(
    snapshot: &ReadTxn,
    keys: usize,
    shape: KeyShape,
    round: usize,
) -> Result<ReadTiming> {
    let mut points = Vec::with_capacity(4096);
    for n in 0..4096 {
        let n = n * 73 % keys;
        let key = shape.key(n * 2);
        let began = Instant::now();
        let got = snapshot.get(&key)?;
        points.push(nanos(began));
        assert_eq!(got, Some(value(round, n)));
    }
    let mut scans = Vec::with_capacity(32);
    for _ in 0..32 {
        let began = Instant::now();
        let got = snapshot.scan(&[], &[255; 64])?;
        scans.push(nanos(began));
        assert_eq!(got.len(), keys);
        for (n, (key, actual)) in got.iter().enumerate() {
            assert_eq!(*key, shape.key(n * 2));
            assert_eq!(*actual, value(round, n));
        }
    }
    let mut batches = Vec::new();
    for batch_shape in ["dense", "sparse", "mixed_missing", "duplicates"] {
        let width = keys.min(128);
        let mut scalar = Vec::new();
        let mut batch = Vec::new();
        let mut timing = BatchTiming {
            shape: batch_shape,
            width,
            samples: 32,
            scalar: Latency::from(vec![1]),
            batch: Latency::from(vec![1]),
            cursor_starts: 0,
            cursor_resolutions: 0,
            scanned_records: 0,
            point_probes: 0,
        };
        for repetition in 0..32 {
            let start = repetition * 29 % (keys - width + 1);
            let mut expected = Vec::with_capacity(width);
            let mut requested = Vec::with_capacity(width);
            for i in 0..width {
                let n = match batch_shape {
                    "dense" => start + i,
                    "duplicates" => (start + i % 8) % keys,
                    _ => (repetition * 29 + i * 73) % keys,
                };
                let missing = batch_shape == "mixed_missing" && i % 2 == 1;
                requested.push(shape.key(n * 2 + usize::from(missing)));
                expected.push((!missing).then(|| value(round, n)));
            }
            if repetition % 2 == 1 {
                requested.reverse();
                expected.reverse();
            }
            let references: Vec<_> = requested.iter().map(Vec::as_slice).collect();
            // Alternate which implementation warms a matched batch's pages.
            for batch_first in [repetition % 2 == 0, repetition % 2 != 0] {
                let began = Instant::now();
                if batch_first {
                    let result = snapshot.get_many(&references)?;
                    batch.push(nanos(began));
                    assert_eq!(result.values, expected);
                    timing.cursor_starts += result.work.cursor_starts;
                    timing.cursor_resolutions += result.work.cursor_resolutions;
                    timing.scanned_records += result.work.scanned_records;
                    timing.point_probes += result.work.point_probes;
                } else {
                    let result: Vec<_> = references
                        .iter()
                        .map(|key| snapshot.get(key))
                        .collect::<Result<_>>()?;
                    scalar.push(nanos(began));
                    assert_eq!(result, expected);
                }
            }
        }
        timing.scalar = Latency::from(scalar);
        timing.batch = Latency::from(batch);
        batches.push(timing);
    }
    Ok(ReadTiming {
        point: Latency::from(points),
        scan: Latency::from(scans),
        batches,
    })
}

pub fn advanced(
    path: &Path,
    layout: Layout,
    keys: usize,
    rounds: usize,
    shape: KeyShape,
    collect: bool,
) -> Result<()> {
    if path.exists()
        || !(16..=MAX_SCAN_ROWS).contains(&keys)
        || !(1..=64).contains(&rounds)
        || keys * (rounds + 1) > MAX_SCAN_RECORDS
    {
        return Err(Error::Invalid(
            "advanced benchmark requires a new journal and bounded sizes",
        ));
    }
    let store = Store::open_layout(path, layout)?;
    let mut operation = 1_u64;
    for start in (0..keys).step_by(MAX_WRITES) {
        let mut tx = store.begin_write()?;
        for n in start..(start + MAX_WRITES).min(keys) {
            tx.put(&shape.key(n * 2), &value(0, n))?;
        }
        tx.commit(operation)?;
        operation += 1;
    }
    let old = store.begin_read()?;
    let mut commits = Vec::new();
    let mut retry = None;
    for round in 1..=rounds {
        for start in (0..keys).step_by(64) {
            let began = Instant::now();
            let mut tx = store.begin_write()?;
            let writes: BTreeMap<_, _> = (start..(start + 64).min(keys))
                .map(|n| (shape.key(n * 2), value(round, n)))
                .collect();
            for (key, value) in &writes {
                tx.put(key, value)?;
            }
            let epoch = tx.commit(operation)?;
            commits.push(nanos(began));
            if round == rounds.saturating_sub(1).max(1) {
                retry = Some((operation, epoch, writes));
            }
            operation += 1;
        }
    }
    diagnostic("installed");
    let current = store.begin_read()?;
    let before_current = read_phase(&current, keys, shape, rounds)?;
    diagnostic("before-current");
    let before_old = read_phase(&old, keys, shape, 0)?;
    diagnostic("before-old");
    let before_stats = store.storage_stats()?;
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let mut checkpoints = Vec::new();
    let mut gc_steps = Vec::new();
    let mut removed = 0;
    let maintenance_started = Instant::now();
    if collect {
        let protected = store.collect_garbage(MAX_GC_WORK)?;
        assert_eq!(protected.removed, 0);
        // Verify that a published replacement generation keeps leased views.
        let start = Instant::now();
        let checkpoint = store.checkpoint()?;
        checkpoints.push((checkpoint, nanos(start)));
        assert_eq!(old.get(&shape.key(0))?, Some(value(0, 0)));
    }
    drop(old);
    drop(current);
    if collect {
        loop {
            let start = Instant::now();
            let result = store.collect_garbage(MAX_GC_WORK)?;
            gc_steps.push(nanos(start));
            assert!(result.examined <= MAX_GC_WORK && result.removed <= result.examined);
            removed += result.removed;
            if result.complete_pass {
                break;
            }
        }
        let start = Instant::now();
        let checkpoint = store.checkpoint()?;
        checkpoints.push((checkpoint, nanos(start)));
        assert_eq!(store.storage_stats()?.retained_versions, keys);
    }
    let maintenance_ns = nanos(maintenance_started);
    diagnostic("maintenance");
    let after_stats = store.storage_stats()?;
    let after = store.begin_read()?;
    let after_current = read_phase(&after, keys, shape, rounds)?;
    diagnostic("after-current");
    let memory_after = ProcessMemorySnapshot::capture().ok();
    let (operation, epoch, writes) = retry.expect("nonempty overwrite workload");
    let start = Instant::now();
    let mut retry = store.begin_write()?;
    for (key, value) in &writes {
        retry.put(key, value)?;
    }
    assert_eq!(retry.commit(operation)?, epoch);
    let retry_ns = nanos(start);
    drop(after);
    drop(store);
    let start = Instant::now();
    let reopened = Store::open_layout(path, layout)?;
    let reopen_ns = nanos(start);
    let recovered = reopened.begin_read()?;
    for n in 0..keys {
        assert_eq!(recovered.get(&shape.key(n * 2))?, Some(value(rounds, n)));
    }
    let recovered_stats = reopened.storage_stats()?;
    assert_eq!(
        recovered_stats.retained_versions,
        after_stats.retained_versions
    );
    if collect {
        assert_eq!(recovered_stats.recovery.replayed_batches, 0);
    }
    diagnostic("reopened");
    let report = serde_json::json!({
        "layout": layout, "keys": keys, "rounds": rounds, "key_shape": shape.name(),
        "collect": collect, "profile": if cfg!(debug_assertions) {"checked-dev"} else {"release"},
        "durability": "sync_all per transaction and complete maintenance record",
        "commit_batches": Latency::from(commits), "before_current": before_current,
        "before_old": before_old, "after_current": after_current,
        "before_stats": before_stats, "after_stats": after_stats, "recovered_stats": recovered_stats,
        "memory_before": memory_before.map(|m| (m.resident_bytes, m.peak_resident_bytes)),
        "memory_after": memory_after.map(|m| (m.resident_bytes, m.peak_resident_bytes)),
        "gc_steps": if gc_steps.is_empty() { None } else { Some(Latency::from(gc_steps)) },
        "removed_versions": removed, "maintenance_ns": maintenance_ns,
        "checkpoints": checkpoints, "reopen_ns": reopen_ns, "historical_retry_ns": retry_ns,
        "notes": "Batch latencies are per request batch, not per key; 32 samples make p99 a maximum. Current snapshots released before collection. Receipt evidence stays in the bounded journal."
    });
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|e| Error::Index(e.to_string()))?
    );
    Ok(())
}
