// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kernel acceptance; language-driven source mapping is tested separately.

use super::*;
use crate::{VectorDistance, VectorIndexBackend, VectorIndexDescriptor, VectorIndexField};
use hawdb_qos::RuntimeWorkRequest;
use hawdb_search::{SearchDocument, SearchQueryError, VectorIndexQuery};
use std::num::NonZeroUsize;

mod incremental_recovery;

#[test]
fn vector_index_publication_funds_the_explicit_task_memory_envelope() {
    use hawdb_search::consumer::RegisteredConsumerProjector::{CanonicalVectorField, HostMapping};
    for (projector, requested, admitted, effective) in [
        (CanonicalVectorField, None, 64 << 20, 64 << 20),
        (CanonicalVectorField, Some(32 << 20), 64 << 20, 32 << 20),
        (CanonicalVectorField, Some(128 << 20), 128 << 20, 128 << 20),
        (HostMapping, Some(128 << 20), 64 << 20, 64 << 20),
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let task = requested.map_or_else(RuntimeTaskContext::default, |bytes| {
            RuntimeTaskContext::default()
                .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(bytes, bytes))
        });
        let observed = std::cell::Cell::new(false);
        let result = db.create_consumer_with_initializer::<
            { hawdb_search::projection_consumer::MAX_CONSUMERS },
            _,
        >(
            id("budget"),
            &fixture.0.join("budget"),
            options(100),
            task.clone(),
            projector,
            |_, _, permit| {
                observed.set(true);
                assert_eq!(permit.request().memory_bytes, admitted);
                let controlled = permit.bind_task_context(task.clone());
                assert_eq!(controlled.memory_reservation().unwrap().memory_bytes(), effective);
                let memory = controlled.reserve_working_memory(1024).unwrap();
                assert!(controlled.reserve_working_memory(effective + 1).is_err());
                drop(memory);
                Err(HawDBError::Execution("budget probe complete".into()))
            },
        );
        assert!(observed.get());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("budget probe complete"));
    }
}

#[test]
fn vector_index_publication_cannot_exceed_host_memory_capacity() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 << 20),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(1, 1),
    );
    db.set_runtime_governor(governor.clone());
    let task = RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(128 << 20, 128 << 20),
    );
    assert!(db.vector_consumer_publication_permit(&task).is_err());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let permit = db.consumer_publication_permit().unwrap();
    assert_eq!(governor.snapshot().admitted_memory_bytes, 64 << 20);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn vector_index_reopen_honors_explicit_working_memory_and_host_capacity() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };

    let fixture = Fixture::new();
    let mut db = fixture.database();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap();
    if let Some(suspension) = &suspension {
        suspension.wait_idle().unwrap();
    }
    let path = fixture.0.join("projection");
    let (consumer, binding) = initialize_bound_index(&mut db, &path);
    let receipt = consumer.projection().stationary_receipt().unwrap();
    let generation = receipt.generation_name().to_owned();
    let epoch = consumer.projection().receipt().source_epoch;
    drop(consumer);
    drop(suspension);
    drop(db);
    let governor = |bytes| {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(bytes),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
            ),
            IoConcurrencyBudget::new(1, 1),
        );
        governor.pin_resources();
        governor
    };
    let task = |bytes| {
        RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(bytes, 0))
    };
    let constrained = governor(96 << 20);
    let mut db = fixture.database();
    db.set_runtime_governor(constrained.clone());
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap();
    if let Some(suspension) = &suspension {
        suspension.wait_idle().unwrap();
    }
    let rejections = constrained.snapshot().admission_rejections;
    assert!(db
        .open_search_projection_consumer_with_task(&id("vectors.embedding"), &path, task(128 << 20))
        .is_err());
    assert_eq!(constrained.snapshot().admission_rejections, rejections + 1);
    assert_eq!(constrained.snapshot().admitted_memory_bytes, 0);
    let result =
        db.open_search_projection_consumer_with_task(&id("vectors.embedding"), &path, task(1024));
    assert!(
        matches!(
            result,
            Err(Error::Database(HawDBError::ResourceBudgetExceeded(_)))
        ),
        "{result:?}"
    );
    assert_eq!(constrained.snapshot().active_background_tasks, 0);
    assert_eq!(constrained.snapshot().admitted_memory_bytes, 0);

    // Default callers retain their existing budget and can retry the same target.
    let consumer = db
        .open_search_projection_consumer(&id("vectors.embedding"), &path)
        .unwrap();
    assert_eq!(
        consumer
            .projection()
            .stationary_receipt()
            .unwrap()
            .generation_name(),
        generation
    );
    drop(consumer);
    assert_eq!(constrained.snapshot().admitted_memory_bytes, 0);
    drop(suspension);
    drop(db);

    let funded = governor(256 << 20);
    let mut db = fixture.database();
    db.set_runtime_governor(funded.clone());
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap();
    if let Some(suspension) = &suspension {
        suspension.wait_idle().unwrap();
    }
    let consumer = db
        .open_search_projection_consumer_with_task(&id("vectors.embedding"), &path, task(128 << 20))
        .unwrap();
    assert_eq!(
        consumer
            .projection()
            .stationary_receipt()
            .unwrap()
            .generation_name(),
        generation
    );
    assert_eq!(consumer.projection().receipt().source_epoch, epoch);
    assert_eq!(consumer.vector_index_descriptor().unwrap(), Some(&binding));
    assert_eq!(funded.snapshot().active_background_tasks, 0);
    drop(consumer);
    assert_eq!(funded.snapshot().admitted_memory_bytes, 0);
}

fn initialize_bound_index(
    db: &mut Database,
    path: &Path,
) -> (SearchProjectionConsumer, VectorIndexDescriptor) {
    db.query_sql("CREATE TABLE vectors (id TEXT PRIMARY KEY, embedding VECTOR(2))")
        .unwrap();
    let binding = db
        .begin_read_transaction()
        .unwrap()
        .resolve_vector_index(
            "vectors.embedding",
            &VectorIndexField::SqlColumn {
                table: "vectors".into(),
                column: "embedding".into(),
            },
            VectorDistance::Cosine,
            VectorIndexBackend::RaBitQ {
                bit_width: 4,
                segment_rows: 64,
                transform_seed: 42,
            },
        )
        .unwrap();
    let consumer = db
        .create_search_projection_consumer(
            id("vectors.embedding"),
            path,
            options(100),
            |_, index| {
                index.bind_vector_index(binding.clone())?;
                for (id, embedding) in [
                    ("a", Some(vec![1.0, 0.0])),
                    ("b", Some(vec![2.0, 0.0])),
                    ("c", Some(vec![0.0, 1.0])),
                    ("d", Some(vec![-1.0, 1.0])),
                    ("e", Some(vec![-1.0, 0.0])),
                    ("null", None),
                ] {
                    index.upsert(SearchDocument {
                        id: id.into(),
                        title: String::new(),
                        content: String::new(),
                        embedding,
                        metadata: Default::default(),
                    })?;
                }
                Ok(())
            },
        )
        .unwrap();
    (consumer, binding)
}

fn request(binding: &VectorIndexDescriptor) -> VectorIndexQuery<'_> {
    VectorIndexQuery {
        descriptor: binding,
        mode: hawdb_core::VectorIndexQueryMode::AnnWindow,
        eligible: None,
        embedding: &[1.0, 0.0],
        candidate_limit: NonZeroUsize::new(8).unwrap(),
        top_k: NonZeroUsize::new(8).unwrap(),
    }
}

fn permit(db: &mut Database, working: u64, result: u64) -> hawdb_qos::RuntimePermit {
    db.search_projection_consumer_read_permit(
        RuntimeWorkRequest::foreground_query(working, result).with_io_wave_slots(1),
    )
    .unwrap()
}

#[test]
fn vector_index_kernel_preserves_signed_distances_and_retained_reopen_results() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let path = fixture.0.join("projection");
    let (consumer, binding) = initialize_bound_index(&mut db, &path);
    let epoch = consumer.projection().receipt().source_epoch;
    let admission = permit(&mut db, 64 << 20, 1 << 20);
    let pin = consumer.pin(&admission).unwrap();
    let output = pin
        .query_vector_index(&admission, Default::default(), request(&binding))
        .unwrap();
    let expected = [0.0, 0.0, 1.0, 1.0 + 1.0 / 2.0f64.sqrt(), 2.0];
    assert_eq!(
        output
            .hits()
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d", "e"]
    );
    for (hit, distance) in output.hits().iter().zip(expected) {
        assert!((hit.distance - distance).abs() < 1e-12);
    }
    assert_eq!(output.report().source_epoch, epoch);
    assert_eq!(output.report().candidate_limit, 8);
    assert_eq!(output.report().reranked_candidate_count, 5);
    assert_eq!(output.report().backend, "rabitq");
    let narrow = pin
        .query_vector_index(
            &admission,
            Default::default(),
            VectorIndexQuery {
                candidate_limit: NonZeroUsize::new(2).unwrap(),
                top_k: NonZeroUsize::MIN,
                ..request(&binding)
            },
        )
        .unwrap();
    assert_eq!(narrow.report().candidate_limit, 2);
    assert_eq!(narrow.report().reranked_candidate_count, 2);
    assert_eq!(narrow.hits().len(), 1);
    assert_eq!(narrow.hits()[0].id, "a");
    assert_eq!(narrow.hits()[0].distance, 0.0);
    drop(narrow);
    drop(pin);
    drop(admission);
    drop(consumer);
    drop(db);
    // Result ownership survives every query-side owner, including the database.
    assert_eq!(output.hits()[4].distance, 2.0);
    let mut db = fixture.database();
    let consumer = db
        .open_search_projection_consumer(&id("vectors.embedding"), &path)
        .unwrap();
    let admission = permit(&mut db, 64 << 20, 1 << 20);
    let pin = consumer.pin(&admission).unwrap();
    let reopened = pin
        .query_vector_index(&admission, Default::default(), request(&binding))
        .unwrap();
    assert_eq!(output.hits(), reopened.hits());
    assert_eq!(output.report().generation, reopened.report().generation);
    assert_eq!(consumer.vector_index_descriptor().unwrap(), Some(&binding));
}

#[test]
fn vector_index_kernel_refuses_invalid_queries_and_recovers_after_admission_failure() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let (consumer, binding) = initialize_bound_index(&mut db, &fixture.0.join("projection"));
    for (working, result) in [(512, 1 << 20), (64 << 20, 1)] {
        let admission = permit(&mut db, working, result);
        let pin = consumer.pin(&admission).unwrap();
        assert!(matches!(
            pin.query_vector_index(&admission, Default::default(), request(&binding)),
            Err(SearchQueryError::Query(HawDBError::ResourceBudgetExceeded(
                _
            ))) | Err(SearchQueryError::Retention(_))
        ));
    }
    let admission = permit(&mut db, 64 << 20, 1 << 20);
    let pin = consumer.pin(&admission).unwrap();
    for embedding in [&[0.0, 0.0][..], &[1.0][..], &[f32::NAN, 0.0][..]] {
        assert!(pin
            .query_vector_index(
                &admission,
                Default::default(),
                VectorIndexQuery {
                    embedding,
                    ..request(&binding)
                }
            )
            .is_err());
    }
    assert!(pin
        .query_vector_index(
            &admission,
            Default::default(),
            VectorIndexQuery {
                candidate_limit: NonZeroUsize::new(1).unwrap(),
                ..request(&binding)
            }
        )
        .is_err());
    let cancelled = RuntimeTaskContext::default();
    cancelled.cancellation().cancel();
    assert!(pin
        .query_vector_index(&admission, cancelled, request(&binding))
        .is_err());
    let mut other = Database::new();
    let foreign = permit(&mut other, 64 << 20, 1 << 20);
    assert!(pin
        .query_vector_index(&foreign, Default::default(), request(&binding))
        .is_err());
    let result = pin
        .query_vector_index(&admission, Default::default(), request(&binding))
        .unwrap();
    assert_eq!(result.hits().len(), 5);
    assert_eq!(result.hits()[4].distance, 2.0);
}

#[test]
fn vector_index_rebuild_recovers_uncertain_selection_without_retargeting_old_reads() {
    for stage in [
        PublicationStage::AfterSelector,
        PublicationStage::AfterSelectorBarrier,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES ($1, $2)",
            &[
                hawdb_core::Value::Int(1),
                hawdb_core::Value::Vector(hawdb_core::Vector::new(&[1.0, 0.0]).unwrap()),
            ],
        )
        .unwrap();
        let path = fixture.0.join("projection");
        let mut consumer = db
            .create_vector_index(
                "uncertain-vector",
                &VectorIndexField::SqlColumn {
                    table: "vectors".into(),
                    column: "embedding".into(),
                },
                &path,
                options(100),
                Default::default(),
            )
            .unwrap();
        let admission = permit(&mut db, 64 << 20, 1 << 20);
        let old = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &admission)
            .unwrap();
        db.query_sql_with_params(
            "UPDATE vectors SET embedding = $1 WHERE id = 1",
            &[hawdb_core::Value::Vector(
                hawdb_core::Vector::new(&[-1.0, 0.0]).unwrap(),
            )],
        )
        .unwrap();
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(db
            .rebuild_vector_index(&mut consumer, Default::default())
            .is_err());
        assert!(
            consumer.projection().stationary_receipt().is_err(),
            "an uncertain selection must not satisfy a host's stationary retry guard"
        );
        let old_generation = old.generation();
        let old_output = old
            .query_candidates(
                &admission,
                Default::default(),
                &[1.0, 0.0],
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
        assert_eq!(old_output.hits()[0].distance, 0.0);
        drop(old_output);
        drop(consumer);
        drop(db);
        let mut db = fixture.database();
        // Recovery cannot take project metadata authority from a retained
        // canonical transaction. Its refusal must not discard pending evidence.
        assert!(matches!(
            db.open_search_projection_consumer(&id("uncertain-vector"), &path),
            Err(Error::Database(HawDBError::BranchBusy { .. }))
        ));
        let old_output = old
            .query_candidates(
                &admission,
                Default::default(),
                &[1.0, 0.0],
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
        assert_eq!(old_output.hits()[0].distance, 0.0);
        drop(old_output);
        drop(old);
        drop(admission);
        let consumer = db
            .open_search_projection_consumer(&id("uncertain-vector"), &path)
            .unwrap();
        let current_permit = permit(&mut db, 64 << 20, 1 << 20);
        let current = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &current_permit)
            .unwrap();
        assert!(current.generation() > old_generation);
        let result = current
            .query_candidates(
                &current_permit,
                Default::default(),
                &[1.0, 0.0],
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
        assert_eq!(result.hits().len(), 1);
        assert_eq!(result.hits()[0].distance, 2.0);
    }
}

#[test]
fn vector_index_refuses_generic_covered_catch_up_before_mapping_or_allocation() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let (consumer, _) = initialize_bound_index(&mut db, &fixture.0.join("projection"));
    let before = super::recovery::ledger(&fixture.0.join("database"));
    let receipt = consumer.projection().receipt();
    let result = db.begin_search_projection_consumer_catch_up(
        &consumer,
        super::covered::config(),
        |_, _, _| panic!("generic mapping cannot replace a schema-bound definition"),
    );
    assert!(matches!(
        result,
        Err(ProjectionConsumerCatchUpError::Consumer(Error::Database(
            HawDBError::Semantic(_)
        )))
    ));
    assert_eq!(consumer.projection().receipt(), receipt);
    assert_eq!(super::recovery::ledger(&fixture.0.join("database")), before);
}

#[test]
fn vector_index_creation_recovers_original_prepared_definition() {
    for stage in [
        PublicationStage::AfterCheckpoint,
        PublicationStage::AfterSelector,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        db.query_sql("CREATE TABLE vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        db.query_sql_with_params(
            "INSERT INTO vectors (id, embedding) VALUES (1, $1)",
            &[hawdb_core::Value::Vector(
                hawdb_core::Vector::new(&[-1.0, 0.0]).unwrap(),
            )],
        )
        .unwrap();
        let path = fixture.0.join("projection");
        let field = VectorIndexField::SqlColumn {
            table: "vectors".into(),
            column: "embedding".into(),
        };
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(db
            .create_vector_index(
                "recover-vector",
                &field,
                &path,
                options(100),
                Default::default()
            )
            .is_err());
        let before = super::recovery::ledger(&fixture.0.join("database"));
        let consumer = db
            .create_vector_index(
                "recover-vector",
                &field,
                &path,
                options(100),
                Default::default(),
            )
            .unwrap();
        let after = super::recovery::ledger(&fixture.0.join("database"));
        assert_eq!(before["high_water"], after["high_water"]);
        assert_eq!(
            before["pending"][0]["operation"],
            after["pending"][0]["operation"]
        );
        let admission = permit(&mut db, 64 << 20, 1 << 20);
        let read = db
            .begin_read_transaction()
            .unwrap()
            .bind_vector_index(&consumer, &admission)
            .unwrap();
        let result = read
            .query_candidates(
                &admission,
                Default::default(),
                &[1.0, 0.0],
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
        assert_eq!(result.hits().len(), 1);
        assert_eq!(result.hits()[0].distance, 2.0);
    }
}
