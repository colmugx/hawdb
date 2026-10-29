// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[path = "../bindings/benchmarks/native_alloc.rs"]
mod native_profile;

use hawdb::{ConcurrentDatabase, Database, Value};
use std::collections::BTreeMap;

fn measure(
    mode: &str,
    case: &str,
    mut query: impl FnMut(&str, &BTreeMap<String, Value>) -> hawdb::Result<hawdb::QueryOutput>,
) {
    let (statement, calls) = match case {
        "point" => (
            "MATCH (n:Boundary {id: $id}) RETURN n.id AS id, n.value AS value",
            256,
        ),
        "scan" => ("MATCH (n:Boundary) RETURN n.id AS id, n.value AS value", 16),
        "update" => (
            "MATCH (n:Boundary {id: $id}) SET n.value = $value RETURN n.id AS id, n.value AS value",
            256,
        ),
        _ => panic!("unknown case"),
    };
    let parameters = BTreeMap::from([
        ("id".to_owned(), Value::Int(7)),
        ("value".to_owned(), Value::Int(42)),
    ]);
    for _ in 0..32 {
        drop(query(statement, &parameters).unwrap());
    }
    let before = native_profile::snapshot();
    for _ in 0..calls {
        let result = query(statement, &parameters).unwrap();
        assert_eq!(result.rows.len(), if case == "scan" { 64 } else { 1 });
        if case != "scan" {
            assert_eq!(result.rows[0].get("id"), Some(&Value::Int(7)));
            assert_eq!(
                result.rows[0].get("value"),
                Some(&Value::Int(if case == "update" { 42 } else { 70 }))
            );
        }
        drop(std::hint::black_box(result));
    }
    let after = native_profile::snapshot();
    println!(
        "{}",
        serde_json::json!({
            "mode": mode,
            "case": case,
            "calls": calls,
            "allocation_calls": after.allocation_calls - before.allocation_calls,
            "allocated_bytes": after.allocated_bytes - before.allocated_bytes,
            "reallocation_calls": after.reallocation_calls - before.reallocation_calls,
            "live_bytes_before": before.live_requested_bytes,
            "live_bytes_after": after.live_requested_bytes,
            "scope": "Rust allocator requests, including successful reallocations; no latency or RSS claim"
        })
    );
}

fn main() {
    let arguments = std::env::args().collect::<Vec<_>>();
    let mode = &arguments[1];
    let case = &arguments[2];
    let mut database = Database::new();
    database.query("CREATE INDEX ON :Boundary(id)").unwrap();
    for id in 0..64 {
        database
            .query_with_params(
                "CREATE (:Boundary {id: $id, value: $value})",
                &BTreeMap::from([
                    ("id".to_owned(), Value::Int(id)),
                    ("value".to_owned(), Value::Int(id * 10)),
                ]),
            )
            .unwrap();
    }
    if mode == "ordinary" {
        measure(mode, case, |text, parameters| {
            database.query_with_params(text, parameters)
        });
    } else {
        assert_eq!(mode, "concurrent");
        let database = ConcurrentDatabase::new(database);
        measure(mode, case, |text, parameters| {
            database.query_with_params(text, parameters)
        });
    }
}
