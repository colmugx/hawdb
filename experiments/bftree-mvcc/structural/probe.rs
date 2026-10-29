// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Copied only into temporary diagnostic builds of upstream Bf-Tree. Node
//! summaries are captured by existing mutation paths; readers never traverse
//! native pointers. Disabled copies compile the recording paths away.

use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

pub(crate) const ENABLED: bool = false;
static COUNTERS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static NODES: OnceLock<Mutex<BTreeMap<usize, Summary>>> = OnceLock::new();

#[derive(Clone, Serialize)]
struct Summary {
    pivots: usize,
    logical_key_bytes: usize,
    stored_postfix_bytes: usize,
    common_leading_bytes: usize,
    distinct_four_byte_previews: usize,
    key_length_histogram: BTreeMap<usize, usize>,
}

#[inline]
pub(crate) fn count(index: usize, amount: usize) {
    if ENABLED {
        COUNTERS[index].fetch_add(amount as u64, Ordering::Relaxed);
    }
}

pub(crate) fn forget(address: usize) {
    if ENABLED {
        NODES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .remove(&address);
    }
}

pub(crate) fn refresh(address: usize, keys: Vec<Vec<u8>>) {
    if !ENABLED {
        return;
    }
    let common = keys.first().zip(keys.last()).map_or(0, |(a, b)| {
        a.iter().zip(b).take_while(|(x, y)| x == y).count()
    });
    let mut histogram = BTreeMap::new();
    for key in &keys {
        *histogram.entry(key.len()).or_default() += 1;
    }
    let summary = Summary {
        pivots: keys.len(),
        logical_key_bytes: keys.iter().map(Vec::len).sum(),
        stored_postfix_bytes: keys.iter().map(|k| k.len().saturating_sub(4)).sum(),
        common_leading_bytes: common,
        distinct_four_byte_previews: keys
            .iter()
            .map(|k| &k[..k.len().min(4)])
            .collect::<BTreeSet<_>>()
            .len(),
        key_length_histogram: histogram,
    };
    NODES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .insert(address, summary);
}

/// All live inner nodes across generations, including trees pinned by old
/// readers. Counters are process-wide and cumulative, including native workers.
pub fn snapshot() -> serde_json::Value {
    let nodes: Vec<_> = NODES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    serde_json::json!({
        "enabled": ENABLED,
        "scope": "all live native inner nodes across tree generations",
        "counters": {
            "lower_bound_calls": COUNTERS[0].load(Ordering::Relaxed),
            "preview_comparisons": COUNTERS[1].load(Ordering::Relaxed),
            "equal_previews": COUNTERS[2].load(Ordering::Relaxed),
            "postfix_comparisons": COUNTERS[3].load(Ordering::Relaxed),
            "postfix_min_operand_bytes": COUNTERS[4].load(Ordering::Relaxed),
            "homogeneous_head_shortcuts": COUNTERS[5].load(Ordering::Relaxed),
        },
        "nodes": nodes,
        "notes": "Operand span is an upper bound, not actual compared bytes. Diagnostic timings are unqualified."
    })
}
