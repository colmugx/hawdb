// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn vector_index_canonical_completion_scopes_full_source_to_declared_field() {
    use crate::{DatabaseConfig, StorageResidencyMode};
    let count = 128usize;
    let mut outcomes = Vec::new();
    for graph in [false, true] {
        let fixture = Fixture::new();
        let mut db = Database::open_with_config(
            fixture.0.join("database"),
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, coarse_cluster BIGINT, fine_cluster BIGINT, embedding VECTOR(4))").unwrap();
        db.query_sql("CREATE INDEX vectors_fine ON vectors (fine_cluster)")
            .unwrap();
        db.query_sql("CREATE INDEX vectors_coarse ON vectors (coarse_cluster)")
            .unwrap();
        db.query_sql("CREATE TABLE noise (id BIGINT PRIMARY KEY)")
            .unwrap();
        db.query("CREATE NODE TABLE Document").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(4)")
            .unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        for id in 0..count {
            let vector = Value::Vector(Vector::new(&[1.0, 0.1, 0.0, 0.0]).unwrap());
            transaction.query_sql_with_params("INSERT INTO vectors (id, coarse_cluster, fine_cluster, embedding) VALUES ($1, $2, $3, $4)", &[
                Value::Int(id as i64), Value::Int((id % 100 / 10) as i64), Value::Int((id % 100) as i64), vector.clone()
            ]).unwrap();
            transaction
                .query_with_params(
                    "CREATE (:Document {embedding: $vector})",
                    &BTreeMap::from([("vector".into(), vector)]),
                )
                .unwrap();
        }
        for id in 0..count * 2 {
            transaction
                .query_sql_with_params(
                    "INSERT INTO noise (id) VALUES ($1)",
                    &[Value::Int(id as i64)],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        db.checkpoint().unwrap();
        let mut options = VectorIndexBuildOptions::default();
        let target_work = if graph {
            1024
        } else {
            (count + 1) * (3 + (usize::BITS - count.leading_zeros()) as usize) + 2
        };
        options.feed.work = NonZeroUsize::new(target_work).unwrap();
        options.max_documents = NonZeroUsize::new(count).unwrap();
        let field = if graph {
            VectorIndexField::NodeProperty {
                table: "Document".into(),
                property: "embedding".into(),
            }
        } else {
            sql_field()
        };
        let result = db.create_vector_index(
            "scoped",
            &field,
            fixture.0.join("index"),
            registration(),
            options.clone(),
        );
        outcomes.push((
            graph,
            result.and_then(|mut consumer| {
                db.rebuild_vector_index(&mut consumer, options)?;
                Ok(consumer.document_count().unwrap())
            }),
        ));
    }
    assert!(
        outcomes
            .iter()
            .all(|(_, result)| matches!(result, Ok(rows) if *rows == count)),
        "{outcomes:?}"
    );
}

#[test]
fn default_vector_index_backend_is_diskann() {
    assert!(matches!(
        VectorIndexBuildOptions::default().backend,
        VectorIndexBackend::DiskAnn {
            max_degree: 32,
            build_beam: 128,
            alpha_milli: 1200,
            lsg: None,
        }
    ));
}

#[test]
fn vector_index_consumer_retains_only_bounded_metadata_and_reopens_its_native_view() {
    use std::io::Read;
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(64))")
        .unwrap();
    for id in 0..32 {
        let mut vector = vec![0.0; 64];
        vector[0] = 1.0;
        vector[1] = id as f32 / 32.0;
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[Value::Int(id), Value::Vector(Vector::new(&vector).unwrap())],
        )
        .unwrap();
    }
    let destination = fixture.0.join("metadata");
    let consumer = db
        .create_vector_index(
            "metadata",
            &sql_field(),
            &destination,
            registration(),
            options(true),
        )
        .unwrap();
    let definition = consumer.vector_index_descriptor().unwrap().unwrap().clone();
    assert_eq!(consumer.document_count().unwrap(), 32);
    assert_eq!(consumer.projection_freshness().unwrap().document_count, 32);
    assert!(consumer.search_index().is_err());
    assert!(consumer.projection().index().is_err());
    let encoded = std::fs::read(destination.join("search_projection.hawdb")).unwrap();
    let end = encoded
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .unwrap()
        + 2;
    let mut text = String::new();
    hawdb_storage::compression::Decoder::new(&encoded[end..])
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    assert!(
        text.len() < 2048,
        "metadata grew with the vector payloads: {}",
        text.len()
    );
    assert!(text
        .lines()
        .any(|line| line.starts_with("vector_document_summary\t32\t32\t")));
    assert!(!text.lines().any(|line| line.starts_with("doc\t")));
    drop(consumer);
    drop(db);
    let mut db = fixture.database();
    let consumer = db
        .open_search_projection_consumer(
            &SearchProjectionConsumerId::new("metadata").unwrap(),
            &destination,
        )
        .unwrap();
    assert_eq!(consumer.document_count().unwrap(), 32);
    assert_eq!(
        consumer.vector_index_descriptor().unwrap(),
        Some(&definition)
    );
    assert!(consumer.search_index().is_err());
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let mut vector = vec![0.0; 64];
    vector[0] = 1.0;
    let output = read
        .query_candidates(
            &permit,
            Default::default(),
            &vector,
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();
    assert_eq!(output.report().backend, "diskann");
    assert_eq!(output.hits().len(), 4);
    assert_eq!(output.hits()[0].distance, 0.0);
}

#[test]
fn diskann_euclidean_keeps_zero_vectors_and_large_finite_distances() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    for (id, vector) in [
        (1, [0.0, 0.0]),
        (2, [1.0, 0.0]),
        (3, [-2.0, 0.0]),
        (4, [f32::MAX, f32::MAX]),
    ] {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[Value::Int(id), Value::Vector(Vector::new(&vector).unwrap())],
        )
        .unwrap();
    }
    let mut build = options(true);
    build.distance = VectorDistance::Euclidean;
    let consumer = db
        .create_vector_index(
            "l2",
            &sql_field(),
            fixture.0.join("l2"),
            registration(),
            build,
        )
        .unwrap();
    let large = VectorDistance::Euclidean
        .distance_slices_with_work(&[1.0, 0.0], &[f32::MAX, f32::MAX], || Ok(()))
        .unwrap();
    assert!(large.is_finite() && large > f64::from(f32::MAX));
    assert_diskann(&mut db, &consumer, &[0.0, 1.0, 3.0, large]);
}

pub(super) fn options(lsg: bool) -> VectorIndexBuildOptions {
    VectorIndexBuildOptions {
        backend: VectorIndexBackend::DiskAnn {
            max_degree: 4,
            build_beam: 16,
            alpha_milli: 1200,
            lsg: lsg.then_some(hawdb_core::VectorIndexLsg {
                sample_size: 32,
                neighbors: 8,
                exponent_milli: 1000,
            }),
        },
        ..Default::default()
    }
}

fn assert_diskann(db: &mut Database, consumer: &SearchProjectionConsumer, expected: &[f64]) {
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(consumer, &permit)
        .unwrap();
    let output = read
        .query_candidates(
            &permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        )
        .unwrap();
    assert_eq!(output.report().backend, "diskann");
    assert_eq!(
        output
            .hits()
            .iter()
            .map(|hit| hit.distance)
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(output.report().reranked_candidate_count, expected.len());
    assert_eq!(output.report().source_epoch, read.source_epoch());
    assert_eq!(output.report().generation, read.generation());
}

fn assert_filtered_language(db: &mut Database, consumer: &SearchProjectionConsumer, graph: bool) {
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let mut read = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(consumer, &permit)
        .unwrap();
    let embedding = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
    let budget = crate::VectorIndexLanguageBudget {
        index_working_bytes: NonZeroUsize::new(32 << 20).unwrap(),
        index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
    };
    let mut rows = Vec::new();
    let collect = |row: crate::executor::Row| {
        rows.push((row["id"].clone(), row["distance"].clone()));
        Ok(())
    };
    if graph {
        let parameters = BTreeMap::from([("q".into(), embedding)]);
        let report = read.query_cypher_streaming(&permit, budget,
            crate::QueryRequest::new("MATCH (d:Document) WHERE d.id > 1 CALL vector_index_search('graph', $q, mode := 'filtered_ann', eligible := d, candidates := 2, topK := 2, maxWork := 100) YIELD node AS n, distance RETURN n.id AS id, distance ORDER BY distance, id").with_params(&parameters), collect).unwrap();
        assert_eq!(report.index().unwrap().backend, "diskann");
        assert_eq!(report.index().unwrap().reranked_candidate_count, 2);
    } else {
        let report = read.query_sql_streaming(&permit, budget, crate::VectorIndexSqlRequest {
            sql: "SELECT v.id, distance FROM vector_index_search('sql', $1, mode => 'filtered_ann', candidates => 2, top_k => 2, max_work => 100) AS v WHERE v.id > 1 ORDER BY distance, v.id",
            parameters: &[embedding], output_limits: Default::default(), task: Default::default(),
        }, collect).unwrap();
        assert_eq!(report.index().backend, "diskann");
        assert_eq!(report.index().reranked_candidate_count, 2);
    }
    assert_eq!(
        rows,
        vec![
            (Value::Int(2), Value::Float(1.0)),
            (Value::Int(3), Value::Float(2.0))
        ]
    );
}

#[test]
fn diskann_registered_sql_and_node_sources_publish_and_reopen_with_optional_lsg() {
    for lsg in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        db.query("CREATE NODE TABLE Document").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(2)")
            .unwrap();
        for (id, coordinates) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
            let vector = Value::Vector(Vector::new(&coordinates).unwrap());
            db.query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
                &[Value::Int(id), vector.clone()],
            )
            .unwrap();
            db.query_with_params(
                "CREATE (:Document {id: $id, embedding: $embedding})",
                &BTreeMap::from([("id".into(), Value::Int(id)), ("embedding".into(), vector)]),
            )
            .unwrap();
        }
        let fields = [
            ("sql", sql_field()),
            (
                "graph",
                VectorIndexField::NodeProperty {
                    table: "Document".into(),
                    property: "embedding".into(),
                },
            ),
        ];
        for (name, field) in &fields {
            let consumer = db
                .create_vector_index(
                    name,
                    field,
                    fixture.0.join(name),
                    registration(),
                    options(lsg),
                )
                .unwrap();
            assert_diskann(&mut db, &consumer, &[0.0, 1.0, 2.0]);
            assert_filtered_language(&mut db, &consumer, name == &"graph");
        }
        drop(db);
        let mut db = fixture.database();
        for (name, _) in fields {
            let consumer = db
                .open_search_projection_consumer(
                    &SearchProjectionConsumerId::new(name).unwrap(),
                    fixture.0.join(name),
                )
                .unwrap();
            assert_diskann(&mut db, &consumer, &[0.0, 1.0, 2.0]);
        }
    }
}

#[test]
fn diskann_successor_keeps_old_mvcc_domain_across_update_delete_null_and_insert() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    for (id, vector) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[Value::Int(id), Value::Vector(Vector::new(&vector).unwrap())],
        )
        .unwrap();
    }
    let mut consumer = db
        .create_vector_index(
            "index",
            &sql_field(),
            fixture.0.join("index"),
            registration(),
            options(true),
        )
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(128 << 20, 4 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let old = db
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    let old_generation = old.generation();
    db.query_sql_with_params(
        "UPDATE vectors SET embedding = $1 WHERE id = 1",
        &[Value::Vector(Vector::new(&[-1.0, 0.0]).unwrap())],
    )
    .unwrap();
    db.query_sql("DELETE FROM vectors WHERE id = 2").unwrap();
    db.query_sql("UPDATE vectors SET embedding = NULL WHERE id = 3")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO vectors (id, embedding) VALUES (4, $1)",
        &[Value::Vector(Vector::new(&[1.0, 0.0]).unwrap())],
    )
    .unwrap();
    assert!(matches!(
        db.begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &permit),
        Err(crate::VectorIndexReadError::SourceEpochMismatch { .. })
    ));
    db.rebuild_vector_index(&mut consumer, options(true))
        .unwrap();
    assert_diskann(&mut db, &consumer, &[0.0, 2.0]);
    let output = old
        .query_candidates(
            &permit,
            Default::default(),
            &[1.0, 0.0],
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        )
        .unwrap();
    assert_eq!(output.report().backend, "diskann");
    assert_eq!(output.report().generation, old_generation);
    assert_eq!(
        output
            .hits()
            .iter()
            .map(|hit| hit.distance)
            .collect::<Vec<_>>(),
        vec![0.0, 1.0, 2.0]
    );
    drop(output);
    drop(old);
    drop(permit);
    drop(consumer);
    drop(db);
    let mut db = fixture.database();
    let consumer = db
        .open_search_projection_consumer(
            &SearchProjectionConsumerId::new("index").unwrap(),
            fixture.0.join("index"),
        )
        .unwrap();
    assert_diskann(&mut db, &consumer, &[0.0, 2.0]);
}
