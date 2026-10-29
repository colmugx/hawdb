// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A canonical snapshot and an index generation are admitted as one read domain.

use super::*;
use crate::{SearchProjectionConsumer, SearchProjectionConsumerReadPin};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{RuntimeAdmissionError, RuntimePermit, RuntimeRetainedMemory};
use hawdb_search::{SearchQueryError, VectorIndexQuery, VectorIndexRetainedOutput};
use std::num::NonZeroUsize;

#[derive(Debug)]
pub enum VectorIndexReadError {
    DatabaseMismatch,
    BranchMismatch,
    SourceEpochMismatch { index: u64, source: u64 },
    SchemaMismatch,
    Query(HawDBError),
    Index(SearchQueryError),
    Admission(RuntimeAdmissionError),
}

impl std::fmt::Display for VectorIndexReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseMismatch => f.write_str("vector index belongs to another database"),
            Self::BranchMismatch => f.write_str("vector index belongs to another branch"),
            Self::SourceEpochMismatch { index, source } => write!(
                f,
                "vector index source epoch {index} differs from canonical snapshot epoch {source}"
            ),
            Self::SchemaMismatch => {
                f.write_str("vector index definition differs from source schema")
            }
            Self::Query(error) => error.fmt(f),
            Self::Index(error) => error.fmt(f),
            Self::Admission(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for VectorIndexReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(error) => Some(error),
            Self::Index(error) => Some(error),
            Self::Admission(error) => Some(error),
            _ => None,
        }
    }
}
impl From<HawDBError> for VectorIndexReadError {
    fn from(error: HawDBError) -> Self {
        Self::Query(error)
    }
}
impl From<SearchQueryError> for VectorIndexReadError {
    fn from(error: SearchQueryError) -> Self {
        Self::Index(error)
    }
}

/// An immutable query domain; it retains no execution or I/O slot while idle.
/// A fresh same-governor permit supplies the resources for each query.
#[derive(Debug)]
pub struct VectorIndexReadTransaction {
    pub(super) source: DatabaseReadTransaction,
    pub(super) pin: SearchProjectionConsumerReadPin,
    pub(super) definition: VectorIndexDescriptor,
    _definition_memory: RuntimeRetainedMemory,
}

impl DatabaseReadTransaction {
    /// Bind this canonical snapshot to exactly one immutable index generation.
    /// A current snapshot cannot silently consume a stale index; callers may
    /// retain an earlier complete domain across source writes and publication.
    pub fn bind_vector_index(
        self,
        consumer: &SearchProjectionConsumer,
        permit: &RuntimePermit,
    ) -> std::result::Result<VectorIndexReadTransaction, VectorIndexReadError> {
        self.store.ensure_usable()?;
        self.config
            .runtime_capabilities
            .require(hawdb_core::RuntimeCapability::VectorSearch)?;
        let pin = consumer.pin(permit)?;
        if self.store.search_projection_database_identity() != Some(pin.source_database_uuid()) {
            return Err(VectorIndexReadError::DatabaseMismatch);
        }
        if self.current_branch.as_ref().map(|branch| branch.info.id)
            != Some(pin.source_branch_uuid())
        {
            return Err(VectorIndexReadError::BranchMismatch);
        }
        let index_epoch = pin.report().source_epoch;
        if index_epoch != self.commit_epoch() {
            return Err(VectorIndexReadError::SourceEpochMismatch {
                index: index_epoch,
                source: self.commit_epoch(),
            });
        }
        // Descriptor copies and schema re-resolution are bounded before either
        // allocates; the surviving definition keeps the original shared grant.
        let definition_memory = permit
            .reserve_retained_memory(
                (hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 4) as u64,
            )
            .map_err(VectorIndexReadError::Admission)?;
        let definition = consumer
            .vector_index_descriptor()?
            .ok_or_else(|| invalid("Consumer has no vector index definition"))?;
        let field = VectorIndexField::from_target(definition.target());
        let resolved = self.resolve_vector_index(
            definition.name(),
            &field,
            definition.distance(),
            definition.backend(),
        )?;
        if &resolved != definition {
            return Err(VectorIndexReadError::SchemaMismatch);
        }
        Ok(VectorIndexReadTransaction {
            source: self,
            pin,
            definition: resolved,
            _definition_memory: definition_memory,
        })
    }
}

impl VectorIndexReadTransaction {
    pub(super) fn require_query_permit(&self, permit: &RuntimePermit) -> Result<()> {
        self.source.store.ensure_usable()?;
        if !self._definition_memory.shares_governor_with(permit) {
            return Err(HawDBError::Execution(
                "vector query permit belongs to another governor".into(),
            ));
        }
        Ok(())
    }

    pub fn definition(&self) -> &VectorIndexDescriptor {
        &self.definition
    }

    pub fn source_epoch(&self) -> u64 {
        self.pin.report().source_epoch
    }

    pub fn generation(&self) -> u64 {
        self.pin.publication_generation()
    }

    /// Execute the bounded candidate kernel in this already validated domain.
    /// Language operators use the same domain for subsequent canonical reads.
    pub fn query_candidates(
        &self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
        embedding: &[f32],
        candidate_limit: NonZeroUsize,
        top_k: NonZeroUsize,
    ) -> std::result::Result<VectorIndexRetainedOutput, VectorIndexReadError> {
        self.require_query_permit(permit)?;
        self.pin
            .query_vector_index(
                permit,
                task,
                VectorIndexQuery {
                    descriptor: &self.definition,
                    embedding,
                    candidate_limit,
                    top_k,
                    mode: hawdb_core::VectorIndexQueryMode::AnnWindow,
                    eligible: None,
                },
            )
            .map_err(Into::into)
    }
}
