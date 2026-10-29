// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual allocation example for owned versus borrowed summary completion.
//! Both paths use the current implementation; historical engine comparisons
//! belong to the separately frozen host_boundary binaries.

#[path = "../bindings/benchmarks/native_alloc.rs"]
mod native_profile;

use hawdb_query::QueryIdentity;
use hawdb_system_sql::{StatementExecution, StatementSummary};
use serde_json::json;
use std::hint::black_box;

struct Input {
    text: String,
    identity: QueryIdentity,
}

impl Input {
    fn new(text: String) -> Self {
        Self {
            identity: QueryIdentity::new("cypher", &text),
            text,
        }
    }
}

struct Case {
    name: &'static str,
    capacity: usize,
    reset: bool,
    failure: bool,
    inputs: Vec<Input>,
}

fn operation(summary: &mut StatementSummary, case: &Case, owned: bool, index: usize) {
    if case.reset {
        *summary = StatementSummary::new(case.capacity);
    }
    let input = black_box(&case.inputs[index % case.inputs.len()]);
    if case.failure {
        summary.record(StatementExecution::failed(
            "cypher",
            &input.text,
            "MATCH",
            &input.identity,
            7,
            "synthetic refusal".to_owned(),
        ));
    } else if owned {
        summary.record(StatementExecution::completed(
            "cypher",
            &input.text,
            "MATCH",
            &input.identity,
            7,
            1,
        ));
    } else {
        summary.record_completed("cypher", &input.text, "MATCH", &input.identity, 7, 1);
    }
}

fn measure(case: &Case, owned: bool, calls: usize, sample: usize) -> serde_json::Value {
    let mut summary = StatementSummary::new(case.capacity);
    operation(&mut summary, case, owned, 0);
    for index in 0..64 {
        operation(&mut summary, case, owned, index);
    }
    let before = native_profile::snapshot();
    for index in 0..calls {
        operation(&mut summary, case, owned, black_box(index));
    }
    let after = native_profile::snapshot();
    let mut snapshot = summary.snapshot();
    for record in &mut snapshot {
        record.last_seen_unix_micros = 0;
    }
    json!({
        "case": case.name,
        "input": if owned { "owned" } else { "borrowed" },
        "sample": sample,
        "calls": calls,
        "allocation_calls": after.allocation_calls - before.allocation_calls,
        "allocated_bytes": after.allocated_bytes - before.allocated_bytes,
        "reallocation_calls": after.reallocation_calls - before.reallocation_calls,
        "deallocation_calls": after.deallocation_calls - before.deallocation_calls,
        "deallocated_bytes": after.deallocated_bytes - before.deallocated_bytes,
        "live_requested_bytes_delta": after.live_requested_bytes as i128 - before.live_requested_bytes as i128,
        "snapshot_without_clock": format!("{snapshot:?}"),
    })
}

fn main() {
    let calls: usize = std::env::args()
        .nth(1)
        .map_or(1000, |value| value.parse().unwrap());
    assert!(calls > 0 && calls.is_multiple_of(2));
    let point =
        "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title";
    let cases = [
        Case {
            name: "hot",
            capacity: 16,
            reset: false,
            failure: false,
            inputs: vec![Input::new(point.to_owned())],
        },
        Case {
            name: "long_hot",
            capacity: 16,
            reset: false,
            failure: false,
            inputs: vec![Input::new(format!(
                "MATCH (n) WHERE n.text = '{}' RETURN n",
                "\u{03bb}".repeat(8192)
            ))],
        },
        Case {
            name: "disabled",
            capacity: 0,
            reset: false,
            failure: false,
            inputs: vec![Input::new(point.to_owned())],
        },
        Case {
            name: "cold",
            capacity: 1,
            reset: true,
            failure: false,
            inputs: vec![Input::new(point.to_owned())],
        },
        Case {
            name: "churn",
            capacity: 1,
            reset: false,
            failure: false,
            inputs: vec![
                Input::new(point.to_owned()),
                Input::new("MATCH (n:Other) RETURN n".to_owned()),
            ],
        },
        Case {
            name: "failed_hot",
            capacity: 16,
            reset: false,
            failure: true,
            inputs: vec![Input::new(point.to_owned())],
        },
    ];
    for sample in 0..3 {
        for case in &cases {
            let order = if sample % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            let first = measure(case, order[0], calls, sample);
            let second = measure(case, order[1], calls, sample);
            assert_eq!(
                first["snapshot_without_clock"],
                second["snapshot_without_clock"]
            );
            assert_eq!(first["live_requested_bytes_delta"], 0);
            assert_eq!(second["live_requested_bytes_delta"], 0);
            println!("{first}");
            println!("{second}");
        }
    }
}
