// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::observer::NoopExecutionObserver;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::{PropertyType, TableKind};
use hawdb_storage::store::GraphStore;

fn fixture(indexed: bool) -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store.create_node_table(&mut catalog, "Item").unwrap();
    for (name, kind) in [
        ("key", PropertyType::Int),
        ("other", PropertyType::Int),
        ("body", PropertyType::String),
    ] {
        store
            .create_property_descriptor(&mut catalog, TableKind::Node, "Item", name, kind, false)
            .unwrap();
    }
    if indexed {
        for property in ["key", "other"] {
            store
                .create_property_index(&mut catalog, "Item", property)
                .unwrap();
        }
    }
    for id in 0..256 {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    ("key".into(), Value::Int(id)),
                    ("other".into(), Value::Int(id + 1000)),
                    ("body".into(), Value::String("x".repeat(8192))),
                ]),
            )
            .unwrap();
    }
    // A same-valued record from another label must never become a candidate.
    store.create_node_table(&mut catalog, "Other").unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Other",
            "key",
            PropertyType::Int,
            false,
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Other",
            BTreeMap::from([("key".into(), Value::Int(2))]),
        )
        .unwrap();
    (catalog, store)
}

fn access(values: &[i64]) -> NodeProjectionAccess {
    NodeProjectionAccess::PropertyValues {
        property: "key".into(),
        values: values.iter().copied().map(Value::Int).collect(),
    }
}

struct Options<'a> {
    task: Option<&'a RuntimeTaskContext>,
    predicate: Option<&'a Predicate>,
    limit: ExecutionLimit,
    budget: usize,
    property: &'static str,
    fail_consumer: bool,
    refuse_work_in_consumer: bool,
}
impl Default for Options<'_> {
    fn default() -> Self {
        Self {
            task: None,
            predicate: None,
            limit: ExecutionLimit::unlimited(),
            budget: 64 * 1024,
            property: "key",
            fail_consumer: false,
            refuse_work_in_consumer: false,
        }
    }
}

fn run(
    catalog: &Catalog,
    store: &GraphStore,
    access: &NodeProjectionAccess,
    options: Options<'_>,
) -> (Result<BatchControl>, Vec<Value>, usize) {
    let budget = NonZeroUsize::new(options.budget).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let source = ledger.account(
        QueryMemoryClass::ExternalRead,
        "indexed test source",
        budget,
    );
    let batch = ledger.account(
        QueryMemoryClass::PipelineBatch,
        "indexed test batch",
        budget,
    );
    let properties = ["key".into(), options.property.into()];
    let items = [Projection {
        name: "result".into(),
        expression: ProjectionExpression::Property {
            variable: "n".into(),
            property: options.property.into(),
        },
    }];
    let mut rows = Vec::new();
    let result = stream_node_projection_scan_batches(
        NodeProjectionScanSpec {
            variable: "n",
            label: "Item",
            access,
            required_properties: &properties,
            predicate: options.predicate,
            items: &items,
        },
        NodeScanContext {
            catalog,
            store,
            execution_limit: options.limit,
            memory_budget: budget,
            memory_account: &source,
            batch_memory_budget: budget,
            batch_memory_account: &batch,
            batch_rows: 1,
            task_context: options.task,
        },
        &NoopExecutionObserver,
        &mut |batch| {
            if options.refuse_work_in_consumer {
                assert!(options.task.unwrap().charge_work(usize::MAX).is_err());
            }
            if options.fail_consumer {
                return Err(HawDBError::Execution("indexed consumer refused".into()));
            }
            for mut row in batch {
                rows.push(row.values.remove("result").unwrap());
            }
            Ok(BatchControl::Continue)
        },
    );
    assert_eq!(
        ledger.snapshot().used_bytes,
        0,
        "all read/heap permits must be released"
    );
    (result, rows, ledger.snapshot().peak_bytes)
}

#[test]
fn bounded_index_projection_visits_candidates_and_merges_duplicate_values_in_id_order() {
    let (catalog, store) = fixture(true);
    for (values, expected) in [
        (vec![2], vec![2]),
        (vec![19, 2, 19, 17, -1], vec![2, 17, 19]),
        (vec![], vec![]),
        (vec![-1, 300], vec![]),
    ] {
        let task = RuntimeTaskContext::default().with_work_limit(32);
        let (result, rows, _) = run(
            &catalog,
            &store,
            &access(&values),
            Options {
                task: Some(&task),
                ..Default::default()
            },
        );
        result.unwrap();
        assert_eq!(
            rows,
            expected.into_iter().map(Value::Int).collect::<Vec<_>>()
        );
        assert!(
            task.work_units() <= 32,
            "must not scan 256 unrelated records"
        );
    }
}

#[test]
fn bounded_index_projection_union_deduplicates_across_indexed_properties() {
    let (catalog, store) = fixture(true);
    let access = NodeProjectionAccess::PropertyUnion {
        branches: vec![
            ExactPropertySeekBranch {
                property: "key".into(),
                values: vec![Value::Int(17), Value::Int(2)],
            },
            ExactPropertySeekBranch {
                property: "other".into(),
                values: vec![Value::Int(1002), Value::Int(1003)],
            },
        ],
    };
    let task = RuntimeTaskContext::default().with_work_limit(32);
    let (result, rows, _) = run(
        &catalog,
        &store,
        &access,
        Options {
            task: Some(&task),
            ..Default::default()
        },
    );
    result.unwrap();
    assert_eq!(rows, vec![Value::Int(2), Value::Int(17), Value::Int(3)]);
}

#[test]
fn bounded_index_projection_preserves_residual_filters_and_missing_index_fallback() {
    let predicate = Predicate::PropertyEq {
        variable: "n".into(),
        property: "key".into(),
        value: Value::Int(17),
    };
    for indexed in [false, true] {
        let (catalog, store) = fixture(indexed);
        let task = RuntimeTaskContext::default().with_work_limit(1024);
        let (result, rows, _) = run(
            &catalog,
            &store,
            &access(&[19, 2, 17]),
            Options {
                task: Some(&task),
                predicate: Some(&predicate),
                ..Default::default()
            },
        );
        result.unwrap();
        assert_eq!(rows, vec![Value::Int(17)]);
        if !indexed {
            assert!(
                task.work_units() >= 256,
                "a missing index is a scan, not an exact miss"
            );
        }
    }
}

#[test]
fn bounded_index_projection_refuses_work_and_cancellation_without_delivering_rows() {
    let (catalog, store) = fixture(true);
    for cancel in [false, true] {
        let task = RuntimeTaskContext::default().with_work_limit(1);
        if cancel {
            task.cancellation().cancel();
        }
        let (result, rows, _) = run(
            &catalog,
            &store,
            &access(&[2]),
            Options {
                task: Some(&task),
                ..Default::default()
            },
        );
        assert!(result.is_err());
        assert!(rows.is_empty());
        assert!(task.charge_work(1).is_err(), "refusal remains sticky");
        let (empty, rows, _) = run(
            &catalog,
            &store,
            &access(&[]),
            Options {
                task: Some(&task),
                ..Default::default()
            },
        );
        assert!(
            empty.is_err(),
            "an empty lookup must propagate the same refusal"
        );
        assert!(rows.is_empty());
    }
}

#[test]
fn bounded_index_projection_stops_before_walking_remaining_postings() {
    let (catalog, store) = fixture(true);
    let task = RuntimeTaskContext::default().with_work_limit(8);
    let (result, rows, _) = run(
        &catalog,
        &store,
        &access(&[255, 2, 17]),
        Options {
            task: Some(&task),
            limit: ExecutionLimit::unlimited().child_for_limit(0, Some(1)),
            ..Default::default()
        },
    );
    result.unwrap();
    assert_eq!(rows, vec![Value::Int(2)]);
}

#[test]
fn bounded_index_projection_preserves_a_caught_work_refusal_at_early_stop() {
    let (catalog, store) = fixture(true);
    let task = RuntimeTaskContext::default().with_work_limit(64);
    let (result, _, _) = run(
        &catalog,
        &store,
        &access(&[2]),
        Options {
            task: Some(&task),
            limit: ExecutionLimit::unlimited().child_for_limit(0, Some(1)),
            refuse_work_in_consumer: true,
            ..Default::default()
        },
    );
    assert!(
        result.is_err(),
        "a caught refusal must not become successful completion"
    );
    assert!(
        task.work_units() < 64,
        "refusal is independent of remaining capacity"
    );
}

#[test]
fn bounded_index_projection_releases_permits_after_payload_and_consumer_refusals() {
    let (catalog, store) = fixture(true);
    for fail_consumer in [false, true] {
        let (result, rows, _) = run(
            &catalog,
            &store,
            &access(&[2, 17]),
            Options {
                budget: if fail_consumer { 64 * 1024 } else { 1024 },
                property: if fail_consumer { "key" } else { "body" },
                fail_consumer,
                ..Default::default()
            },
        );
        assert!(result.is_err());
        assert!(rows.is_empty());
    }
}

#[test]
fn bounded_index_projection_keeps_snapshot_postings_across_update_and_delete() {
    let (mut catalog, mut store) = fixture(true);
    let snapshot = store.snapshot_for_read();
    let filter = PropertyFilter::Eq {
        property: "key".into(),
        value: Value::Int(2),
    };
    store
        .set_node_property(&mut catalog, "Item", Some(&filter), "key", Value::Int(300))
        .unwrap();
    store
        .delete_nodes(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "key".into(),
                value: Value::Int(17),
            }),
            false,
        )
        .unwrap();
    for (read, expected) in [(&snapshot, vec![2, 17]), (&store, vec![300])] {
        let task = RuntimeTaskContext::default().with_work_limit(32);
        let (result, rows, _) = run(
            &catalog,
            read,
            &access(&[300, 17, 2]),
            Options {
                task: Some(&task),
                ..Default::default()
            },
        );
        result.unwrap();
        assert_eq!(
            rows,
            expected.into_iter().map(Value::Int).collect::<Vec<_>>()
        );
    }
}

#[test]
fn bounded_index_projection_matches_complete_scans_for_sparse_duplicate_and_mixed_values() {
    let (indexed_catalog, indexed) = fixture(true);
    let (scan_catalog, scanned) = fixture(false);
    for seed in 0..12 {
        let mut values = (0..8)
            .map(|n| Value::Int((seed * 73 + n * n * 17) % 269 - 5))
            .collect::<Vec<_>>();
        values.extend([
            values[0].clone(),
            Value::Null,
            Value::Float(2.0),
            Value::String("2".into()),
        ]);
        let access = NodeProjectionAccess::PropertyValues {
            property: "key".into(),
            values,
        };
        let task = RuntimeTaskContext::default().with_work_limit(64);
        let (actual, actual_rows, _) = run(
            &indexed_catalog,
            &indexed,
            &access,
            Options {
                task: Some(&task),
                ..Default::default()
            },
        );
        let (expected, expected_rows, _) =
            run(&scan_catalog, &scanned, &access, Options::default());
        actual.unwrap();
        expected.unwrap();
        assert_eq!(actual_rows, expected_rows, "seed={seed}");
    }
}
