// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Fixed native mapping must recover its original C..E publication after G.

use super::*;
use crate::{VectorIndexBuildError, VectorIndexReadTransaction};
use hawdb_core::{Value, Vector};
use std::collections::BTreeMap;

fn insert(db: &mut Database, graph: bool, key: i64, vector: Option<[f32; 2]>) {
    let vector = vector.map_or(Value::Null, |vector| {
        Value::Vector(Vector::new(&vector).unwrap())
    });
    if graph {
        db.query_with_params(
            "CREATE (:Document {id: $id, embedding: $embedding})",
            &BTreeMap::from([("id".into(), Value::Int(key)), ("embedding".into(), vector)]),
        )
        .unwrap();
    } else {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[Value::Int(key), vector],
        )
        .unwrap();
    }
}

fn update(db: &mut Database, graph: bool, key: i64, vector: Option<[f32; 2]>) {
    let vector = vector.map_or(Value::Null, |vector| {
        Value::Vector(Vector::new(&vector).unwrap())
    });
    if graph {
        db.query_with_params(
            "MATCH (d:Document {id: $id}) SET d.embedding = $embedding",
            &BTreeMap::from([("id".into(), Value::Int(key)), ("embedding".into(), vector)]),
        )
        .unwrap();
    } else {
        db.query_sql_with_params(
            "UPDATE vectors SET embedding = $1 WHERE id = $2",
            &[vector, Value::Int(key)],
        )
        .unwrap();
    }
}

fn source_ids(db: &mut Database, graph: bool) -> BTreeMap<i64, String> {
    if graph {
        db.query("MATCH (d:Document) RETURN d.id AS id, id(d) AS native")
            .unwrap()
            .rows
            .into_iter()
            .map(|row| {
                let (Value::Int(id), Value::Int(native)) = (&row["id"], &row["native"]) else {
                    panic!("expected native node identity");
                };
                (*id, format!("n:{native:016x}"))
            })
            .collect()
    } else {
        use hawdb_storage::relational::{
            encode_relational_primary_key, RelationalKey, RelationalValue,
        };
        (1..=6)
            .map(|id| {
                let bytes =
                    encode_relational_primary_key(&RelationalKey(vec![RelationalValue::BigInt(
                        id,
                    )]))
                    .unwrap();
                (
                    id,
                    format!(
                        "r:{}",
                        bytes
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    ),
                )
            })
            .collect()
    }
}

fn hits(
    read: &VectorIndexReadTransaction,
    permit: &hawdb_qos::RuntimePermit,
) -> Vec<(String, f64)> {
    let output = read
        .query_candidates(
            permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();
    assert_eq!(output.report().backend, "diskann");
    output
        .hits()
        .iter()
        .map(|hit| (hit.id.clone(), hit.distance))
        .collect()
}

fn original_graph(fixture: &Fixture) -> (PathBuf, Vec<u8>) {
    for root in std::fs::read_dir(&fixture.0).unwrap().flatten() {
        let manifest = root
            .path()
            .join("search_projection.out_of_core.manifest.hawdb");
        if !manifest.is_file() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap();
        let name = value["body"]["segments"][0]["rabitq_artifact_file"]
            .as_str()
            .unwrap();
        let path = root.path().join(name);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"HDANN002"));
        return (path, bytes);
    }
    panic!("expected the native graph closure");
}

fn create_source(db: &mut Database, graph: bool) -> VectorIndexField {
    if graph {
        db.query("CREATE NODE TABLE Document").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
            .unwrap();
        VectorIndexField::NodeProperty {
            table: "Document".into(),
            property: "embedding".into(),
        }
    } else {
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        VectorIndexField::SqlColumn {
            table: "vectors".into(),
            column: "embedding".into(),
        }
    }
}

fn check_publication(stage: PublicationStage, graph: bool, reopen: bool) {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let field = create_source(&mut db, graph);
    for (id, vector) in [
        (1, Some([1.0, 0.0])),
        (2, Some([0.0, 1.0])),
        (3, Some([-1.0, 0.0])),
        (4, None),
    ] {
        insert(&mut db, graph, id, vector);
    }
    let path = fixture.0.join("projection");
    let mut consumer = db
        .create_vector_index(
            "incremental-recovery",
            &field,
            &path,
            options(1000),
            Default::default(),
        )
        .unwrap();
    let initial = consumer.projection().receipt();
    let (graph_path, graph_bytes) = original_graph(&fixture);
    let initial_ids = source_ids(&mut db, graph);
    let expected_old = vec![
        (initial_ids[&1].clone(), 0.0),
        (initial_ids[&2].clone(), 1.0),
        (initial_ids[&3].clone(), 2.0),
    ];
    let mut admission = permit(&mut db, 64 << 20, 1 << 20);
    let mut old = Some(
        db.begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &admission)
            .unwrap(),
    );
    assert_eq!(hits(old.as_ref().unwrap(), &admission), expected_old);
    update(&mut db, graph, 1, Some([0.0, 1.0]));
    update(&mut db, graph, 3, None);
    if graph {
        db.query("MATCH (d:Document {id: 2}) DELETE d").unwrap();
    } else {
        db.query_sql("DELETE FROM vectors WHERE id = 2").unwrap();
    }
    insert(&mut db, graph, 5, Some([1.0, 0.0]));
    let e = db.commit_epoch().unwrap();
    let e_ids = source_ids(&mut db, graph);
    PUBLICATION_FAILURE.set(Some((stage, false)));
    let failure = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap_err();
    assert!(
        matches!(&failure, VectorIndexBuildError::Consumer(Error::Database(HawDBError::Storage(message))) if message == &format!("injected consumer publication failure at {stage:?}")),
        "{stage:?}, graph={graph}, reopen={reopen}: {failure:?}"
    );
    assert_eq!(PUBLICATION_FAILURE.get(), None);
    let before = super::super::recovery::ledger(&fixture.0.join("database"));
    assert_eq!(hits(old.as_ref().unwrap(), &admission), expected_old);
    update(&mut db, graph, 1, Some([-1.0, 0.0]));
    insert(&mut db, graph, 6, Some([0.0, 1.0]));
    let g = db.commit_epoch().unwrap();
    let g_ids = source_ids(&mut db, graph);
    let selected = !matches!(
        stage,
        PublicationStage::BeforeCheckpoint
            | PublicationStage::AfterCheckpoint
            | PublicationStage::BeforeSelector
    );
    if reopen {
        // A canonical read transaction owns its original branch runtime. Cold
        // recovery begins after those original runtime leases are released.
        drop(old.take());
        drop(admission);
        drop(consumer);
        drop(db);
        db = fixture.database();
        consumer = db
            .open_search_projection_consumer(&id("incremental-recovery"), &path)
            .unwrap();
        admission = permit(&mut db, 64 << 20, 1 << 20);
    } else {
        db.resolve_search_projection_consumer_publication(&mut consumer)
            .unwrap();
        assert_eq!(hits(old.as_ref().unwrap(), &admission), expected_old);
    }
    let recovered_epoch = if selected { e } else { initial.source_epoch };
    assert_eq!(
        consumer.canonical_vector_source_epoch().unwrap(),
        Some(recovered_epoch),
        "{stage:?}, graph={graph}, reopen={reopen}"
    );
    assert_eq!(
        consumer.projection().receipt().binding.registration_uuid,
        initial.binding.registration_uuid
    );
    let after = super::super::recovery::ledger(&fixture.0.join("database"));
    assert_eq!(before["high_water"], after["high_water"]);
    let pin = consumer.pin(&admission).unwrap();
    let definition = consumer.vector_index_descriptor().unwrap().unwrap();
    let output = pin
        .query_vector_index(&admission, Default::default(), request(definition))
        .unwrap();
    let expected = if selected {
        vec![(e_ids[&5].clone(), 0.0), (e_ids[&1].clone(), 1.0)]
    } else {
        expected_old.clone()
    };
    assert_eq!(
        output
            .hits()
            .iter()
            .map(|hit| (hit.id.clone(), hit.distance))
            .collect::<Vec<_>>(),
        expected
    );
    drop(output);
    drop(pin);
    assert!(
        matches!(db.begin_read_transaction().unwrap().bind_vector_index(&consumer, &admission), Err(crate::VectorIndexReadError::SourceEpochMismatch {index,source}) if index == recovered_epoch && source == g)
    );
    let report = db
        .catch_up_vector_index(&mut consumer, Default::default())
        .unwrap();
    assert_eq!(report.base_epoch, recovered_epoch);
    assert_eq!(report.source_epoch, g);
    let current = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &admission)
        .unwrap();
    assert_eq!(
        hits(&current, &admission),
        vec![
            (g_ids[&5].clone(), 0.0),
            (g_ids[&6].clone(), 1.0),
            (g_ids[&1].clone(), 2.0)
        ]
    );
    if let Some(old) = &old {
        assert_eq!(hits(old, &admission), expected_old);
    }
    assert_eq!(std::fs::read(graph_path).unwrap(), graph_bytes);
}

fn qualify(reopen: bool) {
    for stage in [
        PublicationStage::BeforeCheckpoint,
        PublicationStage::AfterCheckpoint,
        PublicationStage::BeforeSelector,
        PublicationStage::AfterSelector,
        PublicationStage::AfterSelectorBarrier,
        PublicationStage::BeforeAcknowledge,
        PublicationStage::BeforeRegistry,
        PublicationStage::AfterRegistry,
    ] {
        for graph in [false, true] {
            check_publication(stage, graph, reopen);
            eprintln!(
                "canonical-vector-publication-v1 stage={stage:?} graph={graph} reopen={reopen}"
            );
        }
    }
}

#[test]
fn vector_index_incremental_publication_faults_resolve_the_original_epoch_after_source_writes() {
    qualify(false);
}

#[test]
fn vector_index_incremental_publication_faults_reopen_the_original_epoch_after_source_writes() {
    qualify(true);
}

#[cfg(feature = "test-support")]
#[test]
fn vector_index_incremental_prepared_fd_pressure_preserves_custody_and_releases_execution() {
    for graph in [false, true] {
        for after_intent in [false, true] {
            for rebuild in [false, true] {
                let fixture = Fixture::new();
                let root = fixture.0.join("database");
                let mut db = Database::open_with_config(
                    &root,
                    crate::DatabaseConfig {
                        max_open_files: 128,
                        strict_capacity_limit: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let field = create_source(&mut db, graph);
                for (id, vector) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
                    insert(&mut db, graph, id, Some(vector));
                }
                let mut consumer = db
                    .create_vector_index(
                        "incremental-pressure",
                        &field,
                        fixture.0.join("projection"),
                        options(1000),
                        Default::default(),
                    )
                    .unwrap();
                let initial = consumer.projection().receipt();
                let initial_operation = super::super::recovery::ledger(&root)["high_water"]
                    .as_u64()
                    .unwrap();
                let source = source_ids(&mut db, graph);
                let admission = permit(&mut db, 64 << 20, 1 << 20);
                let old = db
                    .begin_read_transaction()
                    .unwrap()
                    .bind_vector_index(&consumer, &admission)
                    .unwrap();
                drop(admission);
                update(&mut db, graph, 1, Some([0.0, 1.0]));
                let governor = db.consumer_governor().unwrap();
                let (failure, held) =
                    super::super::recovery::with_ledger_pressure_and_selector_refusal(
                        &mut db,
                        &root,
                        move |row| {
                            row["operation"].as_u64().unwrap() > initial_operation
                                && row["candidate_seal"].is_object()
                                && row["publication_intent"].is_object() == after_intent
                        },
                        |db| {
                            if rebuild {
                                db.rebuild_vector_index(&mut consumer, Default::default())
                            } else {
                                db.catch_up_vector_index(&mut consumer, Default::default())
                                    .map(|_| ())
                            }
                        },
                    );
                assert!(
                    matches!(&failure, Err(VectorIndexBuildError::Consumer(Error::Database(HawDBError::Storage(message)))) if message.contains("injected consumer publication failure")),
                    "graph={graph} after_intent={after_intent} rebuild={rebuild}: {failure:?}"
                );
                assert_eq!(db.file_descriptor_metrics().unwrap().budget_rejections, 0);
                assert!(db.file_descriptor_metrics().unwrap().high_water > 128);
                assert_eq!(governor.snapshot().active_background_tasks, 0);
                assert_eq!(governor.snapshot().active_cpu_slots, 0);
                let pending = super::super::recovery::ledger(&root);
                held.borrow_mut().clear();
                // Registry refresh must not replace the original retained native
                // birth/prepared owner with custody reconstructed from its label.
                db.runtime
                    .get_mut()
                    .unwrap()
                    .projection_consumers
                    .unavailable = true;
                db.reload_consumer_registry_if_unavailable().unwrap();
                update(&mut db, graph, 1, Some([-1.0, 0.0]));
                db.resolve_search_projection_consumer_publication(&mut consumer)
                    .unwrap();
                assert_eq!(consumer.projection().receipt(), initial);
                assert_eq!(
                    super::super::recovery::ledger(&root)["high_water"],
                    pending["high_water"]
                );
                db.catch_up_vector_index(&mut consumer, Default::default())
                    .unwrap();
                let admission = permit(&mut db, 64 << 20, 1 << 20);
                let current = db
                    .begin_read_transaction()
                    .unwrap()
                    .bind_vector_index(&consumer, &admission)
                    .unwrap();
                assert_eq!(
                    hits(&current, &admission),
                    vec![
                        (source[&2].clone(), 1.0),
                        (source[&1].clone(), 2.0),
                        (source[&3].clone(), 2.0)
                    ]
                );
                assert_eq!(
                    hits(&old, &admission),
                    vec![
                        (source[&1].clone(), 0.0),
                        (source[&2].clone(), 1.0),
                        (source[&3].clone(), 2.0)
                    ]
                );
                eprintln!("canonical-vector-pressure-v1 graph={graph} after_intent={after_intent} rebuild={rebuild}");
            }
        }
    }
}
