// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Index queries share the Consumer's native custody, I/O and retention grants.

use super::OpenedRoot;
use crate::{SearchQueryResult, VectorIndexQuery, VectorIndexRetainedOutput};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::RuntimePermit;

impl OpenedRoot {
    pub(in crate::out_of_core) fn vector_index_query(
        &self,
        request: VectorIndexQuery<'_>,
        publication_generation: u64,
        permit: &RuntimePermit,
        task: &RuntimeTaskContext,
    ) -> SearchQueryResult<VectorIndexRetainedOutput> {
        #[cfg(feature = "vector-search")]
        {
            self.execute_vector_index_query(request, publication_generation, permit, task)
        }
        #[cfg(not(feature = "vector-search"))]
        {
            let _ = (request, publication_generation, permit, task);
            Err(hawdb_core::HawDBError::CapabilityUnavailable {
                capability: hawdb_core::RuntimeCapability::VectorSearch,
            }
            .into())
        }
    }

    #[cfg(feature = "vector-search")]
    fn execute_vector_index_query(
        &self,
        request: VectorIndexQuery<'_>,
        publication_generation: u64,
        permit: &RuntimePermit,
        task: &RuntimeTaskContext,
    ) -> SearchQueryResult<VectorIndexRetainedOutput> {
        use crate::build_memory::read::{QueryIoScope, ReadAdmission, ReadPermit};
        use crate::build_memory::{BuildMemory, ACCOUNT_METADATA_BYTES};
        use crate::lexical_projection::QueryContext;
        use crate::out_of_core::{query_memory, query_output, SearchOutOfCoreMetrics};
        use crate::{HawDBError, VectorIndexHit, VectorIndexQueryReport};

        let memory = BuildMemory::new(task)?;
        let _metadata = memory.spool.reserve(ACCOUNT_METADATA_BYTES)?;
        let preflight = QueryContext {
            memory: &memory.spool,
            task,
            execution: Some(permit),
            io_scope: None,
        };
        preflight.checkpoint()?;
        if !self.stationary_immutable {
            return Err(HawDBError::Execution(
                "vector index requires an immutable Consumer generation".into(),
            )
            .into());
        }
        let source_epoch = self.reader.source_graph_commit_epoch().ok_or_else(|| {
            HawDBError::StorageIntegrity("vector index generation has no source epoch".into())
        })?;
        {
            let _definition_memory = memory
                .spool
                .reserve(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2)?;
            let binding = self.reader.vector_index_descriptor()?.ok_or_else(|| {
                HawDBError::Semantic("Consumer has no vector index definition".into())
            })?;
            if &binding != request.descriptor {
                return Err(HawDBError::Semantic(
                    "vector index definition differs from the pinned generation".into(),
                )
                .into());
            }
        }
        if request.mode == hawdb_core::VectorIndexQueryMode::AnnWindow && request.eligible.is_some()
        {
            return Err(HawDBError::Semantic(
                "ann_window cannot consume an eligible domain".into(),
            )
            .into());
        }
        if request.mode != hawdb_core::VectorIndexQueryMode::Exact
            && request.top_k > request.candidate_limit
        {
            return Err(
                HawDBError::Semantic("vector top-k exceeds its candidate window".into()).into(),
            );
        }
        for (requested, limit, option) in [
            (
                if request.mode != hawdb_core::VectorIndexQueryMode::Exact {
                    request.candidate_limit.get()
                } else {
                    0
                },
                self.reader.config.max_vector_candidates.get(),
                "SearchOutOfCoreConfig.max_vector_candidates",
            ),
            (
                request.top_k.get(),
                self.reader.config.max_score_entries.get(),
                "SearchOutOfCoreConfig.max_score_entries",
            ),
        ] {
            if requested > limit {
                return Err(hawdb_core::ResourceBudgetError {
                    resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
                    scope: hawdb_core::ResourceBudgetScope::Operator,
                    owner: "vector index query window".into(),
                    memory_class: None,
                    requested: requested as u64,
                    used: 0,
                    budget: limit as u64,
                    budget_option: option,
                    message: "vector query window exceeds its configured limit".into(),
                }
                .into());
            }
        }
        crate::vector_index::validate_embedding(
            Some(request.descriptor),
            request.embedding,
            || preflight.checkpoint(),
        )?;
        if let Some(domain) = request.eligible {
            domain.validate(request.descriptor, request.embedding, request.mode)?;
        } else if request.mode.requires_eligible_domain() {
            return Err(HawDBError::Semantic(
                "vector mode requires a complete eligible domain".into(),
            )
            .into());
        }
        let frame = query_output::Frame::with_view_bytes(
            preflight,
            permit,
            std::mem::size_of::<VectorIndexRetainedOutput>(),
        )?;
        let output = query_output::Memory(Some(&frame));
        let mut metrics = SearchOutOfCoreMetrics::default();
        let scan = if request.mode == hawdb_core::VectorIndexQueryMode::Exact {
            self.reader.rank_exact_index_domain(request, preflight)?
        } else if request
            .eligible
            .is_some_and(|domain| domain.report().eligible_vectors == 0)
        {
            crate::out_of_core::vector_serving::VectorScoreScan::default()
        } else {
            // Scratch precedes the one I/O wave so cleanup can obtain fresh I/O.
            let stage = if self.reader.visibility.is_empty() && request.eligible.is_none() {
                None
            } else {
                let disk_bytes = self
                    .reader
                    .config
                    .max_candidate_spill_bytes
                    .get()
                    .checked_add(crate::out_of_core::CANDIDATE_FILE_HEADER.len() as u64)
                    .ok_or_else(|| {
                        HawDBError::Execution("vector candidate spill bound overflow".into())
                    })?;
                let admission = ReadAdmission {
                    memory: &memory,
                    task,
                    permit: ReadPermit::Governed(permit),
                };
                let mut stage = self
                    ._native_custody
                    .as_ref()
                    .ok_or_else(|| {
                        HawDBError::Execution("vector index lacks original native custody".into())
                    })?
                    .validation_stage(&self.reader.root, &admission)?;
                stage.reserve_disk(disk_bytes);
                Some(stage)
            };
            let scratch = stage.as_ref().and_then(|stage| stage.scratch_directory());
            let io = QueryIoScope::acquire(task)?;
            let query = QueryContext {
                io_scope: Some(&io),
                ..preflight
            };
            let mut predicates = query_memory::Predicates::new(&Default::default(), Some(query))?;
            let candidates = self.reader.build_candidate_set(
                crate::out_of_core::candidate_set::CandidateFilter {
                    predicates: &predicates.predicates,
                    eligible: request.eligible,
                },
                &mut predicates.report,
                &mut metrics,
                Some(query),
                scratch.as_deref(),
                output,
            )?;
            self.reader
                .scan_index_distances(request, &candidates, query, &mut metrics)?
        };
        let mut hits = output.vec(scan.scores.len())?;
        for (id, negative_distance) in &scan.scores {
            preflight.checkpoint()?;
            hits.push(VectorIndexHit {
                id: output.text(id)?,
                distance: -*negative_distance,
            });
        }
        hits.sort_unstable_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.id.cmp(&right.id))
        });
        let report = VectorIndexQueryReport {
            index_name: output.text(request.descriptor.name())?,
            schema_digest: *request.descriptor.schema_digest(),
            source_epoch,
            generation: publication_generation,
            backend: if request.mode == hawdb_core::VectorIndexQueryMode::Exact {
                "exact"
            } else {
                match scan.backend.as_str() {
                    "hawdb_diskann_out_of_core_candidate_graph" => "diskann",
                    "hawdb_rabitq_out_of_core_candidate_projection" => "rabitq",
                    _ if scan.generated_candidate_count == 0 => request.descriptor.backend().name(),
                    _ => {
                        return Err(HawDBError::StorageIntegrity(
                            "vector index used an unexpected candidate backend".into(),
                        )
                        .into())
                    }
                }
            },
            mode: request.mode,
            filter_position: request.mode.filter_position(),
            eligibility: request.eligible.map(|domain| domain.report()),
            candidate_limit: if request.mode == hawdb_core::VectorIndexQueryMode::Exact {
                0
            } else {
                request.candidate_limit.get()
            },
            top_k: request.top_k.get(),
            generated_candidate_count: scan.generated_candidate_count,
            reranked_candidate_count: scan.reranked_candidate_count,
        };
        frame
            .finish_vector_index(hits, report, metrics)
            .map_err(Into::into)
    }
}
