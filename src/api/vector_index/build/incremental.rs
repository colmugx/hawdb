// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Fixed field mapping over one complete C..E feed and its pinned canonical E.

use super::*;
use hawdb_storage::store::ProjectionFeedItem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexUpdateReport {
    pub base_epoch: u64,
    pub source_epoch: u64,
    pub mapped_candidates: usize,
    pub upserted_vectors: usize,
    /// Missing/NULL keys are retraction candidates; some were never indexed.
    pub retraction_candidates: usize,
    pub feed_work: usize,
    pub feed_items: usize,
    pub feed_bytes: usize,
}

impl Database {
    /// Hydrate only changed native keys and retain every unchanged graph segment.
    /// The definition must match the build options. A feed gap or schema change
    /// requires an explicit rebuild; no partial cursor is acknowledged.
    ///
    /// A typed retryable background-admission refusal or pre-effect `BranchBusy`
    /// may be retried with bounded backoff only after checking that this same
    /// Consumer still has its completed generation and source epoch. Check the
    /// task's cancellation/deadline between attempts. The qualification host
    /// uses 25 ms backoff for at most 30 seconds; the library does not retry
    /// automatically. Pending or uncertain publication must use Consumer
    /// recovery, rather than being treated as a pre-effect refusal.
    pub fn catch_up_vector_index(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
        mut build: VectorIndexBuildOptions,
    ) -> std::result::Result<VectorIndexUpdateReport, VectorIndexBuildError> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::VectorSearch)?;
        build.bind_memory_limit()?;
        checkpoint(&build.task)?;
        self.search_projection_consumer_readiness(consumer, None)
            .map_err(VectorIndexBuildError::Consumer)?;
        let base_epoch = consumer.canonical_vector_source_epoch()?.ok_or_else(|| {
            invalid("incremental vector maintenance requires a fixed canonical baseline")
        })?;
        let definition = consumer
            .vector_index_descriptor()?
            .ok_or_else(|| invalid("Consumer has no vector definition"))?;
        if definition.backend() != build.backend || definition.distance() != build.distance {
            return Err(invalid(
                "incremental maintenance cannot change the bound vector algorithm",
            )
            .into());
        }
        if self.commit_epoch()? == base_epoch {
            return Ok(VectorIndexUpdateReport {
                base_epoch,
                source_epoch: base_epoch,
                mapped_candidates: 0,
                upserted_vectors: 0,
                retraction_candidates: 0,
                feed_work: 0,
                feed_items: 0,
                feed_bytes: 0,
            });
        }
        let mut input_error = None;
        let mut producer_memory = None;
        let mut report = None;
        let mut admission_error = None;
        let result = self.rebuild_consumer_with_initializer(
            consumer,
            build.task.clone(),
            &mut admission_error,
            |source, index, permit, definition| {
                let mapped = (|| {
                    producer_memory = Some(
                        permit
                            .reserve_retained_memory(build.memory_bytes.get() as u64)
                            .map_err(VectorIndexBuildError::Admission)?,
                    );
                    report = Some(build_delta(source, index, definition, base_epoch, &build)?);
                    Ok(())
                })();
                mapped.map_err(|error| {
                    input_error = Some(error);
                    HawDBError::Execution("incremental canonical vector mapping failed".into())
                })
            },
        );
        drop(producer_memory);
        if let Some(error) = input_error {
            return Err(error);
        }
        if let Some(error) = admission_error {
            return Err(VectorIndexBuildError::Admission(error));
        }
        result.map_err(VectorIndexBuildError::Consumer)?;
        report.ok_or_else(|| invalid("completed vector update has no mapping report").into())
    }
}

fn build_delta(
    source: &mut DatabaseReadTransaction,
    index: &mut SearchIndex,
    previous: &VectorIndexDescriptor,
    base_epoch: u64,
    options: &VectorIndexBuildOptions,
) -> std::result::Result<VectorIndexUpdateReport, VectorIndexBuildError> {
    let ledger = QueryMemoryLedger::new(options.memory_bytes);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "incremental vector source",
        options.memory_bytes,
    );
    let _metadata = account.reserve(add(
        options.source.metadata_bytes.get(),
        hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 4,
    )?)?;
    let field = VectorIndexField::from_target(previous.target());
    let binding = source.resolve_vector_index(
        previous.name(),
        &field,
        previous.distance(),
        previous.backend(),
    )?;
    if &binding != previous {
        return Err(invalid("canonical vector schema changed; rebuild required").into());
    }
    let (statement, _statement_memory) = source_statement(source, &binding, &account)?;
    index.bind_vector_index(binding.clone())?;
    let mut cursor = source
        .store
        .capture_projection_feed(base_epoch, options.feed)?;
    let mut candidates = BTreeMap::<String, bool>::new();
    let mut candidate_memory = account.reserve(0)?;
    let mut documents = account.reserve(0)?;
    let mut upserts = 0;
    let row_bytes = add(
        options
            .max_row_payload_bytes
            .get()
            .checked_mul(2)
            .ok_or_else(overflow)?,
        add(
            options
                .source
                .key_bytes
                .get()
                .checked_mul(16)
                .ok_or_else(overflow)?,
            4096,
        )?,
    )?;
    while !cursor.is_complete() {
        cursor.visit_next(options.feed, |item| {
            checkpoint(&options.task)?;
            if let ProjectionFeedItem::End { source_epoch } = item {
                if source_epoch != source.commit_epoch() {
                    return Err(invalid(
                        "vector feed end differs from canonical hydration snapshot",
                    ));
                }
                return Ok(());
            }
            let applicable = match (binding.target(), item) {
                (VectorIndexTarget::NodeProperty { .. }, ProjectionFeedItem::GraphNode { .. }) => {
                    true
                }
                (
                    VectorIndexTarget::Relational { table: target, .. },
                    ProjectionFeedItem::RelationalKey { table, .. },
                ) => target == table,
                _ => false,
            };
            if !applicable {
                return Ok(());
            }
            let _row = account.reserve(row_bytes)?;
            let id = match (binding.target(), item) {
                (
                    VectorIndexTarget::NodeProperty { .. },
                    ProjectionFeedItem::GraphNode { node_id, .. },
                ) => format!("n:{node_id:016x}"),
                (
                    VectorIndexTarget::Relational { table: target, .. },
                    ProjectionFeedItem::RelationalKey { table, key, .. },
                ) if target == table => relational_id(key, options.source.key_bytes.get())?,
                _ => unreachable!("only applicable keys require mapping memory"),
            };
            if candidates.contains_key(&id) {
                return Ok(());
            }
            if candidates.len() >= options.max_documents.get() {
                return Err(hawdb_core::ResourceBudgetError {
                    resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
                    scope: hawdb_core::ResourceBudgetScope::Operator,
                    owner: "incremental vector candidates".into(),
                    memory_class: None,
                    requested: 1,
                    used: candidates.len() as u64,
                    budget: options.max_documents.get() as u64,
                    budget_option: "VectorIndexBuildOptions.max_documents",
                    message: "complete vector delta exceeds candidate admission".into(),
                }
                .into());
            }
            candidate_memory.grow(add(2048, id.len().checked_mul(3).ok_or_else(overflow)?)?)?;
            let task = options
                .task
                .clone()
                .with_memory_reservation(RuntimeMemoryReservation::new(
                    account.available_bytes() as u64,
                    options.max_row_payload_bytes.get() as u64,
                ));
            let limits = QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(options.max_row_payload_bytes.get()),
            };
            match item {
                ProjectionFeedItem::GraphNode { node_id, .. } => {
                    let node = i64::try_from(node_id)
                        .map_err(|_| invalid("node ID exceeds query integer range"))?;
                    let parameters = BTreeMap::from([("node".into(), Value::Int(node))]);
                    source.query_with_params_streaming_context(
                        &statement,
                        &parameters,
                        limits,
                        &task,
                        |row| {
                            append(
                                index,
                                row.get("embedding"),
                                &id,
                                &binding,
                                options,
                                &mut documents,
                                &mut upserts,
                            )
                        },
                    )?;
                }
                ProjectionFeedItem::RelationalKey { key, .. } => {
                    let parameters = key
                        .0
                        .iter()
                        .map(super::super::super::relational_value_to_query_value)
                        .collect::<Result<Vec<_>>>()?;
                    let rows = source.query_sql_with_params_options_context(
                        &statement,
                        &parameters,
                        limits,
                        &task,
                    )?;
                    if rows.rows.len() > 1 {
                        return Err(invalid("canonical vector key hydrated multiple rows"));
                    }
                    if let Some(row) = rows.rows.first() {
                        append(
                            index,
                            row.get("embedding"),
                            &id,
                            &binding,
                            options,
                            &mut documents,
                            &mut upserts,
                        )?;
                    }
                }
                _ => unreachable!("only fixed native vector keys are hydrated"),
            }
            let present = index.document(&id).is_some();
            candidates.insert(id, present);
            Ok(())
        })?;
    }
    let usage = cursor.usage();
    let mapped_candidates = candidates.len();
    let retraction_candidates = candidates.values().filter(|present| !**present).count();
    let deletes = candidates
        .into_iter()
        .filter_map(|(id, present)| (!present).then_some(id))
        .collect();
    index.finish_canonical_vector_source(source.commit_epoch(), Some(base_epoch), deletes)?;
    Ok(VectorIndexUpdateReport {
        base_epoch,
        source_epoch: source.commit_epoch(),
        mapped_candidates,
        upserted_vectors: upserts,
        retraction_candidates,
        feed_work: usage.work,
        feed_items: usage.items,
        feed_bytes: usage.bytes,
    })
}

fn relational_id(key: &hawdb_storage::relational::RelationalKey, limit: usize) -> Result<String> {
    let bytes = key
        .0
        .iter()
        .try_fold(std::mem::size_of_val(key), |bytes, value| {
            add(
                add(bytes, std::mem::size_of_val(value))?,
                value.estimated_payload_bytes(),
            )
        })?;
    if key.0.len() > 32 || bytes > limit {
        return Err(hawdb_core::ResourceBudgetError {
            resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
            scope: hawdb_core::ResourceBudgetScope::Operator,
            owner: "incremental vector primary key".into(),
            memory_class: None,
            requested: bytes as u64,
            used: 0,
            budget: limit as u64,
            budget_option: "VectorIndexBuildOptions.source.key_bytes",
            message: "vector key exceeds pre-encoding admission".into(),
        }
        .into());
    }
    let encoded = hawdb_storage::relational::encode_relational_primary_key(key)
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    let mut id = String::with_capacity(add(2, encoded.len().checked_mul(2).ok_or_else(overflow)?)?);
    id.push_str("r:");
    use std::fmt::Write;
    for byte in encoded {
        write!(&mut id, "{byte:02x}").expect("String write");
    }
    Ok(id)
}
