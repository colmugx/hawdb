// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::read::{VectorIndexReadError, VectorIndexReadTransaction};
use super::*;
use crate::{QueryRequest, QueryStreamReport, SearchProjectionConsumerReadPin};
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use hawdb_executor::{
    ExternalReadOperator, VectorIndexExecutionOutput, VectorIndexExecutionRequest,
    VectorIndexExecutionRow, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
};
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};
use hawdb_search::{
    SearchQueryError, VectorIndexQuery, VectorIndexQueryReport, VectorIndexRetainedOutput,
};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A sub-budget of the caller's permit. The canonical query uses the remaining
/// working/result allowance, so the two execution trees cannot each spend the
/// complete permit. This is independent of the explicit ANN candidate window.
#[derive(Debug, Clone, Copy)]
pub struct VectorIndexLanguageBudget {
    pub index_working_bytes: NonZeroUsize,
    pub index_result_bytes: NonZeroUsize,
}

#[derive(Debug)]
pub struct VectorIndexLanguageReport {
    pub query: QueryStreamReport,
    index: Option<Arc<VectorIndexRetainedOutput>>,
    _metadata_memory: RuntimeRetainedMemory,
}

impl VectorIndexLanguageReport {
    /// None means execution did not reach the index operator, for example LIMIT 0.
    pub fn index(&self) -> Option<&VectorIndexQueryReport> {
        self.index.as_deref().map(VectorIndexRetainedOutput::report)
    }

    /// Logical index reads and spill counters from the retained execution.
    pub fn index_metrics(&self) -> Option<&crate::SearchOutOfCoreMetrics> {
        self.index
            .as_deref()
            .map(VectorIndexRetainedOutput::metrics)
    }

    /// Runtime memory still owned by the index output, excluding query rows.
    pub fn index_retained_capacity_bytes(&self) -> Option<u64> {
        self.index
            .as_deref()
            .map(VectorIndexRetainedOutput::retained_capacity_bytes)
    }
}

impl VectorIndexLanguageBudget {
    fn split(
        self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
    ) -> std::result::Result<(RuntimeTaskContext, RuntimeTaskContext), VectorIndexReadError> {
        if task.has_resource_controllers() {
            return Err(invalid("vector index language request requires an unbound task").into());
        }
        let grant = RuntimeMemoryReservation::new(
            permit.request().memory_bytes,
            permit.request().result_bytes,
        );
        let grant = task
            .memory_reservation()
            .map_or(grant, |parent| grant.intersect(parent));
        let remaining_working = grant
            .memory_bytes()
            .checked_sub(self.index_working_bytes.get() as u64)
            .filter(|bytes| *bytes > 0);
        let remaining_result = grant
            .result_bytes()
            .checked_sub(self.index_result_bytes.get() as u64)
            .filter(|bytes| *bytes > 0);
        let (Some(working), Some(result)) = (remaining_working, remaining_result) else {
            let (requested, available, option) = if remaining_working.is_none() {
                (
                    self.index_working_bytes.get() as u64,
                    grant.memory_bytes(),
                    "VectorIndexLanguageBudget.index_working_bytes",
                )
            } else {
                (
                    self.index_result_bytes.get() as u64,
                    grant.result_bytes(),
                    "VectorIndexLanguageBudget.index_result_bytes",
                )
            };
            return Err(HawDBError::from(hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::Query,
                owner: "vector index language budget".into(),
                memory_class: None,
                requested: requested.saturating_add(1),
                used: 0,
                budget: available,
                budget_option: option,
                message:
                    "vector index sub-budget must leave canonical query working and result memory"
                        .into(),
            })
            .into());
        };
        let query_task = permit.bind_task_context(
            task.clone()
                .with_memory_reservation(RuntimeMemoryReservation::new(working, result)),
        );
        let index_task = task.with_memory_reservation(RuntimeMemoryReservation::new(
            self.index_working_bytes.get() as u64,
            self.index_result_bytes.get() as u64,
        ));
        Ok((index_task, query_task))
    }
}

impl VectorIndexReadTransaction {
    /// Execute an ordinary Cypher pipeline in the already bound read domain.
    /// Rows are delivered through the engine's validated consumer boundary.
    pub fn query_cypher_streaming(
        &mut self,
        permit: &RuntimePermit,
        budget: VectorIndexLanguageBudget,
        request: QueryRequest<'_>,
        consumer: impl FnMut(crate::executor::Row) -> Result<()>,
    ) -> std::result::Result<VectorIndexLanguageReport, VectorIndexReadError> {
        self.require_query_permit(permit)?;
        let task = request
            .task_context
            .or(self.source.task_context.as_ref())
            .cloned()
            .unwrap_or_default();
        let (index_task, query_task) = budget.split(permit, task)?;
        if !matches!(
            self.definition.target(),
            VectorIndexTarget::NodeProperty { .. }
        ) {
            return Err(
                invalid("Cypher vector index search requires a node property index").into(),
            );
        }
        let metadata_memory = permit
            .reserve_retained_memory(4096)
            .map_err(VectorIndexReadError::Admission)?;
        let mut provider = IndexProvider {
            pin: &self.pin,
            definition: &self.definition,
            permit,
            task: index_task,
            output: None,
            error: None,
        };
        let query = self.source.query_request_streaming_with_external(
            request.with_task_context(&query_task),
            &mut provider,
            consumer,
        );
        if let Some(error) = provider.error {
            return Err(VectorIndexReadError::Index(error));
        }
        Ok(VectorIndexLanguageReport {
            query: query?,
            index: provider.output,
            _metadata_memory: metadata_memory,
        })
    }
}

struct IndexProvider<'a> {
    pin: &'a SearchProjectionConsumerReadPin,
    definition: &'a VectorIndexDescriptor,
    permit: &'a RuntimePermit,
    task: RuntimeTaskContext,
    output: Option<Arc<VectorIndexRetainedOutput>>,
    error: Option<SearchQueryError>,
}

impl ExternalReadOperator for IndexProvider<'_> {
    fn vector_index_definition(
        &mut self,
        index: &str,
    ) -> Result<(VectorIndexDescriptor, hawdb_core::VectorIndexReadIdentity)> {
        if index != self.definition.name() {
            return Err(invalid(
                "query index differs from the bound index definition",
            ));
        }
        // VectorIndexReadTransaction construction binds the canonical snapshot
        // and this pin to the same epoch. Returning the pin's identity lets the
        // domain reject reuse with a different pin; it does not independently
        // establish snapshot consistency for this transaction.
        Ok((
            self.definition.clone(),
            self.pin.vector_index_read_identity(),
        ))
    }

    fn execute_vector_seed(
        &mut self,
        _: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        Err(invalid(
            "legacy vector search is not an index-bound operator",
        ))
    }

    fn execute_vector_index(
        &mut self,
        request: VectorIndexExecutionRequest<'_>,
    ) -> Result<Box<dyn VectorIndexExecutionOutput>> {
        self.execute_kernel(request)
            .map(|output| Box::new(NodeCandidates(output)) as Box<dyn VectorIndexExecutionOutput>)
    }
}

impl IndexProvider<'_> {
    fn execute_kernel(
        &mut self,
        request: VectorIndexExecutionRequest<'_>,
    ) -> Result<Arc<VectorIndexRetainedOutput>> {
        request.resources.checkpoint()?;
        if self.output.is_some() {
            return Err(invalid(
                "one index query operator is supported per read request",
            ));
        }
        if request.index != self.definition.name() {
            return Err(invalid(
                "query index differs from the bound index definition",
            ));
        }
        let candidate_limit =
            NonZeroUsize::new(if request.mode == hawdb_core::VectorIndexQueryMode::Exact {
                request.top_k
            } else {
                request.candidate_limit
            })
            .ok_or_else(|| invalid("empty vector candidate window"))?;
        let top_k = NonZeroUsize::new(request.top_k).ok_or_else(|| invalid("empty vector topK"))?;
        let coordinate_bytes = (request.embedding.dimension().get() as usize)
            .checked_mul(4)
            .ok_or_else(|| invalid("vector embedding size overflow"))?;
        let grant = self
            .task
            .memory_reservation()
            .expect("language sub-budget is explicit");
        let working = grant
            .memory_bytes()
            .min(request.resources.max_working_memory_bytes.get() as u64);
        let result = grant
            .result_bytes()
            .min(request.resources.result.max_memory_bytes.get() as u64);
        let kernel_working = working
            .checked_sub(coordinate_bytes as u64)
            .filter(|n| *n > 0)
            .ok_or_else(|| hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::Operator,
                owner: "vector index embedding".into(),
                memory_class: None,
                requested: coordinate_bytes as u64,
                used: 0,
                budget: working,
                budget_option: "VectorIndexLanguageBudget.index_working_bytes",
                message: "embedding leaves no working memory for the index".into(),
            })?;
        let embedding = request.embedding.coordinates().collect::<Vec<_>>();
        let output = match self.pin.query_vector_index(
            self.permit,
            self.task
                .clone()
                .with_memory_reservation(RuntimeMemoryReservation::new(kernel_working, result)),
            VectorIndexQuery {
                descriptor: self.definition,
                embedding: &embedding,
                candidate_limit,
                top_k,
                mode: request.mode,
                eligible: request.eligible,
            },
        ) {
            Ok(output) => Arc::new(output),
            Err(error) => {
                self.error = Some(error);
                return Err(HawDBError::Execution(
                    "bound vector index execution failed".into(),
                ));
            }
        };
        self.output = Some(Arc::clone(&output));
        Ok(output)
    }
}

struct NodeCandidates(Arc<VectorIndexRetainedOutput>);
impl VectorIndexExecutionOutput for NodeCandidates {
    fn row_count(&self) -> usize {
        self.0.hits().len()
    }
    fn row(&self, ordinal: usize) -> Result<VectorIndexExecutionRow> {
        let hit = self
            .0
            .hits()
            .get(ordinal)
            .ok_or_else(|| invalid("vector candidate ordinal is out of range"))?;
        let id = hit
            .id
            .strip_prefix("n:")
            .filter(|id| id.len() == 16)
            .and_then(|id| u64::from_str_radix(id, 16).ok())
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(
                    "vector index contains an invalid canonical node identity".into(),
                )
            })?;
        Ok(VectorIndexExecutionRow {
            node_id: hawdb_storage::NodeId(id),
            distance: hit.distance,
        })
    }
}

mod sql;
pub use sql::{VectorIndexSqlReport, VectorIndexSqlRequest};
