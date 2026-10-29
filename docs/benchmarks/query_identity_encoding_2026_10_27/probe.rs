// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "allocation-probe")]
mod native_alloc;

use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

struct Case {
    name: &'static str,
    language: &'static str,
    text: String,
}

fn cases() -> Vec<Case> {
    let point =
        "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title";
    vec![
        Case {
            name: "point",
            language: "cypher",
            text: point.to_owned(),
        },
        Case {
            name: "sql",
            language: "sql",
            text: "SELECT id, title FROM memories WHERE id = $1".to_owned(),
        },
        Case {
            name: "mixed_language",
            language: " CYPHER ",
            text: point.to_owned(),
        },
        Case {
            name: "trimmed_language",
            language: "\u{00a0}cypher\u{2003}",
            text: point.to_owned(),
        },
        Case {
            name: "unicode_language",
            language: "\u{03a9}",
            text: "SELECT 1".to_owned(),
        },
        Case {
            name: "mixed_unicode_language",
            language: "\u{03a9}SQL",
            text: "SELECT 1".to_owned(),
        },
        Case {
            name: "empty",
            language: "cypher",
            text: String::new(),
        },
        Case {
            name: "unparsed_text",
            language: "sql",
            text: "SELECT 'a\0b\u{03bb}".to_owned(),
        },
        Case {
            name: "long_literal",
            language: "cypher",
            text: format!(
                "MATCH (n) WHERE n.text = '{}' RETURN n",
                "x".repeat(16 * 1024)
            ),
        },
    ]
}

fn verify(case: &Case) -> serde_json::Value {
    let first = baseline::QueryIdentity::new(case.language, &case.text);
    let second = candidate::QueryIdentity::new(case.language, &case.text);
    assert_eq!(first.normalized_query(), second.normalized_query());
    assert_eq!(first.query_digest(), second.query_digest());
    assert_eq!(first.query_text_hash(), second.query_text_hash());
    let cache_key = baseline::normalize_query_for_plan_cache(case.language, &case.text);
    assert_eq!(
        cache_key,
        candidate::normalize_query_for_plan_cache(case.language, &case.text)
    );
    json!({
        "case": case.name,
        "language": case.language,
        "text": case.text,
        "normalized_query": first.normalized_query(),
        "query_digest": first.query_digest(),
        "query_text_hash": first.query_text_hash(),
        "cache_key": cache_key,
    })
}

fn baseline_identity(language: &str, text: &str) {
    drop(black_box(baseline::QueryIdentity::new(
        black_box(language),
        black_box(text),
    )));
}

fn candidate_identity(language: &str, text: &str) {
    drop(black_box(candidate::QueryIdentity::new(
        black_box(language),
        black_box(text),
    )));
}

fn baseline_cache(language: &str, text: &str) {
    drop(black_box(baseline::normalize_query_for_plan_cache(
        black_box(language),
        black_box(text),
    )));
}

fn candidate_cache(language: &str, text: &str) {
    drop(black_box(candidate::normalize_query_for_plan_cache(
        black_box(language),
        black_box(text),
    )));
}

fn measure(
    case: &Case,
    producer: &str,
    mode: &str,
    sample: usize,
    calls: u64,
    operation: fn(&str, &str),
) {
    for _ in 0..64 {
        operation(case.language, &case.text);
    }
    #[cfg(feature = "allocation-probe")]
    let before = native_alloc::snapshot();
    let started = Instant::now();
    for _ in 0..calls {
        operation(case.language, &case.text);
    }
    let elapsed_ns = started.elapsed().as_nanos();
    #[cfg(feature = "allocation-probe")]
    let profile = {
        let after = native_alloc::snapshot();
        json!({
            "allocation_calls": after.allocation_calls - before.allocation_calls,
            "allocated_bytes": after.allocated_bytes - before.allocated_bytes,
            "reallocation_calls": after.reallocation_calls - before.reallocation_calls,
            "deallocation_calls": after.deallocation_calls - before.deallocation_calls,
            "deallocated_bytes": after.deallocated_bytes - before.deallocated_bytes,
            "live_bytes_before": before.live_requested_bytes,
            "live_bytes_after": after.live_requested_bytes,
            "process_peak_requested_bytes": after.process_peak_requested_bytes,
        })
    };
    #[cfg(not(feature = "allocation-probe"))]
    let profile = serde_json::Value::Null;
    println!(
        "{}",
        json!({
            "case": case.name, "producer": producer, "mode": mode, "sample": sample,
            "calls": calls, "elapsed_ns": elapsed_ns, "allocation_profile": profile,
        })
    );
}

#[cfg(feature = "allocation-probe")]
fn measure_retained<T>(case: &Case, producer: &str, sample: usize, construct: impl FnOnce() -> T) {
    let before = native_alloc::snapshot();
    let value = black_box(construct());
    let retained = native_alloc::snapshot();
    drop(black_box(value));
    let released = native_alloc::snapshot();
    assert_eq!(released.live_requested_bytes, before.live_requested_bytes);
    println!(
        "{}",
        json!({
            "case": case.name, "producer": producer, "mode": "retained_identity", "sample": sample,
            "calls": 1,
            "allocation_calls": retained.allocation_calls - before.allocation_calls,
            "allocated_bytes": retained.allocated_bytes - before.allocated_bytes,
            "reallocation_calls": retained.reallocation_calls - before.reallocation_calls,
            "retained_requested_bytes": retained.live_requested_bytes - before.live_requested_bytes,
            "live_bytes_after_release": released.live_requested_bytes - before.live_requested_bytes,
        })
    );
}

fn main() {
    let cases = cases();
    if std::env::args().any(|argument| argument == "--vectors") {
        for case in &cases {
            println!("{}", verify(case));
        }
        return;
    }
    let calls = std::env::args()
        .nth(1)
        .map_or(10_000, |argument| argument.parse().unwrap());
    for case in &cases {
        drop(verify(case));
    }
    for sample in 0..5 {
        for case in &cases {
            for mode in ["identity", "cache_key"] {
                let producers = if sample % 2 == 0 {
                    ["baseline", "candidate"]
                } else {
                    ["candidate", "baseline"]
                };
                for producer in producers {
                    let operation = match (producer, mode) {
                        ("baseline", "identity") => baseline_identity,
                        ("candidate", "identity") => candidate_identity,
                        ("baseline", "cache_key") => baseline_cache,
                        ("candidate", "cache_key") => candidate_cache,
                        _ => unreachable!(),
                    };
                    measure(case, producer, mode, sample, calls, operation);
                    #[cfg(feature = "allocation-probe")]
                    if mode == "identity" {
                        if producer == "baseline" {
                            measure_retained(case, producer, sample, || {
                                baseline::QueryIdentity::new(
                                    black_box(case.language),
                                    black_box(&case.text),
                                )
                            });
                        } else {
                            measure_retained(case, producer, sample, || {
                                candidate::QueryIdentity::new(
                                    black_box(case.language),
                                    black_box(&case.text),
                                )
                            });
                        }
                    }
                }
            }
        }
    }
}
