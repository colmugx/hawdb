// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    SearchProjectionConsumer, SearchProjectionConsumerId, SearchProjectionConsumerOptions,
    VectorIndexBuildOptions, VectorIndexField, VectorIndexReadTransaction,
};
use hawdb_core::Vector;
use hawdb_qos::RuntimeWorkRequest;
use std::num::{NonZeroU64, NonZeroUsize};

mod faults;

fn create_source(database: &mut Database, graph: bool) -> VectorIndexField {
    if graph {
        database.query("CREATE NODE TABLE Document").unwrap();
        database
            .query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
            .unwrap();
        VectorIndexField::NodeProperty {
            table: "Document".into(),
            property: "embedding".into(),
        }
    } else {
        database
            .query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        VectorIndexField::SqlColumn {
            table: "vectors".into(),
            column: "embedding".into(),
        }
    }
}

fn ids(read: &VectorIndexReadTransaction, permit: &hawdb_qos::RuntimePermit) -> Vec<String> {
    read.query_candidates(
        permit,
        Default::default(),
        &[1.0, 0.0],
        NonZeroUsize::new(8).unwrap(),
        NonZeroUsize::new(8).unwrap(),
    )
    .unwrap()
    .hits()
    .iter()
    .map(|hit| hit.id.clone())
    .collect()
}

fn key(id: i64) -> String {
    use hawdb_storage::relational::{
        encode_relational_primary_key, RelationalKey, RelationalValue,
    };
    let bytes =
        encode_relational_primary_key(&RelationalKey(vec![RelationalValue::BigInt(id)])).unwrap();
    format!(
        "r:{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn current_ids(database: &mut Database, consumer: &SearchProjectionConsumer) -> Vec<String> {
    let permit = database
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = database
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(consumer, &permit)
        .unwrap();
    ids(&read, &permit)
}

fn insert(database: &mut Database, graph: bool, id: i64, vector: [f32; 2]) {
    let embedding = Value::Vector(Vector::new(&vector).unwrap());
    if graph {
        database
            .query_with_params(
                "CREATE (:Document {id: $id, embedding: $embedding})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("embedding".into(), embedding),
                ]),
            )
            .unwrap();
    } else {
        database
            .query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                &[Value::Int(id), embedding],
            )
            .unwrap();
    }
}

fn source_keys(database: &mut Database, graph: bool) -> BTreeMap<i64, String> {
    if !graph {
        return (1..=4).map(|id| (id, key(id))).collect();
    }
    database
        .query("MATCH (d:Document) RETURN d.id AS id, id(d) AS native")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            let (Value::Int(id), Value::Int(native)) = (&row["id"], &row["native"]) else {
                panic!("unexpected node identity");
            };
            (*id, format!("n:{native:016x}"))
        })
        .collect()
}

#[test]
fn vector_index_incremental_acknowledgement_survives_loss_of_all_uncovered_writes() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for graph in [false, true] {
            let fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            let field = create_source(&mut database, graph);
            for (id, vector) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
                insert(&mut database, graph, id, vector);
            }
            let before = source_keys(&mut database, graph);
            let target = fixture.root.join("projection");
            let id = SearchProjectionConsumerId::new("vectors").unwrap();
            let mut consumer = database
                .create_vector_index(
                    "vectors",
                    &field,
                    &target,
                    SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap()),
                    VectorIndexBuildOptions::default(),
                )
                .unwrap();
            let permit = database
                .search_projection_consumer_read_permit(
                    RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
                )
                .unwrap();
            let old = database
                .begin_read_transaction()
                .unwrap()
                .bind_vector_index(&consumer, &permit)
                .unwrap();
            let embedding = Value::Vector(Vector::new(&[0.0, 1.0]).unwrap());
            if graph {
                database
                    .query_with_params(
                        "MATCH (d:Document {id: 1}) SET d.embedding = $embedding",
                        &BTreeMap::from([("embedding".into(), embedding)]),
                    )
                    .unwrap();
                database
                    .query("MATCH (d:Document {id: 2}) DELETE d")
                    .unwrap();
            } else {
                database
                    .query_sql_with_params(
                        "UPDATE vectors SET embedding = $1 WHERE id = 1",
                        &[embedding],
                    )
                    .unwrap();
                database
                    .query_sql("DELETE FROM vectors WHERE id = 2")
                    .unwrap();
            }
            insert(&mut database, graph, 4, [1.0, 0.0]);
            let after = source_keys(&mut database, graph);
            let report = database
                .catch_up_vector_index(&mut consumer, Default::default())
                .unwrap();
            assert_eq!(report.upserted_vectors, 2);
            assert_eq!(
                ids(&old, &permit),
                vec![before[&1].clone(), before[&2].clone(), before[&3].clone()]
            );
            assert_eq!(
                current_ids(&mut database, &consumer),
                vec![after[&4].clone(), after[&1].clone(), after[&3].clone()]
            );
            let acknowledged = fixture
                .model
                .capture()
                .unwrap()
                .crash(&CrashPlan::default())
                .unwrap();
            assert!(!acknowledged.directory_paths().any(|path| path
                .components()
                .any(|component| component.as_os_str() == ".consumer-producer")));
            drop(old);
            drop(permit);
            drop(consumer);
            drop(database);
            let root = fixture.root.clone();
            drop(fixture);
            let image = MaterializedImage(root);
            acknowledged.materialize(&image).unwrap();
            let mut recovered = open(&image, durability);
            assert_eq!(recovered.commit_epoch().unwrap(), report.source_epoch);
            let consumer = recovered
                .open_search_projection_consumer(&id, &target)
                .unwrap();
            assert_eq!(
                consumer.canonical_vector_source_epoch().unwrap(),
                Some(report.source_epoch)
            );
            assert_eq!(
                current_ids(&mut recovered, &consumer),
                vec![after[&4].clone(), after[&1].clone(), after[&3].clone()]
            );
            let rows = if graph {
                recovered
                    .query("MATCH (d:Document) RETURN d.id AS id ORDER BY id")
                    .unwrap()
            } else {
                recovered
                    .query_sql("SELECT id FROM vectors ORDER BY id")
                    .unwrap()
            };
            assert_eq!(
                rows.rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(1), Value::Int(3), Value::Int(4)]
            );
        }
    }
}
