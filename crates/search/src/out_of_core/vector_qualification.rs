// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Qualification binds the complete selected graph and its visibility, without
//! opening artifacts or rehydrating documents after reader construction.

use super::*;
use crate::{VectorProjectionQualificationIdentity, VectorProjectionResourceEvidence};
use hawdb_integrity::IntegrityHasher;
use std::io;

struct DigestWriter(IntegrityHasher);

impl io::Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SearchOutOfCoreReader {
    pub(super) fn has_diskann_projection(&self) -> bool {
        self.segments.iter().any(|segment| {
            segment
                .rabitq_projection
                .as_ref()
                .is_some_and(vector_reader::Projection::is_diskann)
        })
    }

    pub(super) fn diskann_qualification(
        &self,
    ) -> Option<(
        VectorProjectionQualificationIdentity,
        VectorProjectionResourceEvidence,
    )> {
        let dimension = self.manifest.embedding_dimension?;
        let mut reference = None;
        let mut stored_vectors = 0usize;
        let mut resources = VectorProjectionResourceEvidence::default();
        if self.segments.len() != self.manifest.segments.len() {
            return None;
        }
        for (segment, selected) in self.segments.iter().zip(&self.manifest.segments) {
            if segment.content_segment_id != selected.segment_id {
                return None;
            }
            let vectors = segment
                .layout
                .segments
                .iter()
                .try_fold(0usize, |total, range| {
                    total.checked_add(range.vectors.entry_count)
                })?;
            let Some(projection) = segment.rabitq_projection.as_ref() else {
                if vectors != 0 {
                    return None;
                }
                continue;
            };
            if !projection.is_diskann()
                || reference.is_some_and(|previous: &vector_reader::Projection| {
                    !previous.compatible(projection)
                })
            {
                return None;
            }
            let manifest = projection.manifest();
            if manifest.dimension != dimension
                || manifest.document_count != vectors
                || manifest.identity.embedding_model != self.manifest.embedding_model
                || manifest.identity.embedding_version != self.manifest.embedding_version
                || manifest.identity.generation > self.manifest.generation
                || manifest
                    .identity
                    .source_epoch
                    .zip(self.manifest.source_graph_commit_epoch)
                    .is_some_and(|(segment, active)| segment > active)
            {
                return None;
            }
            reference = Some(projection);
            stored_vectors = stored_vectors.checked_add(vectors)?;
            resources.segment_count = resources
                .segment_count
                .checked_add(manifest.segment_count)?;
            resources.requested_segment_rows = resources
                .requested_segment_rows
                .max(manifest.requested_segment_rows);
            resources.admitted_segment_rows = resources
                .admitted_segment_rows
                .max(manifest.admitted_segment_rows);
            // Graph construction is sequential. These are admitted construction
            // bounds, not measured RSS or a sum of concurrently live builders.
            resources.configured_build_working_bytes = resources
                .configured_build_working_bytes
                .max(manifest.configured_build_working_bytes);
            resources.peak_build_working_bytes = resources
                .peak_build_working_bytes
                .max(manifest.peak_build_working_bytes);
            resources.projection_payload_bytes = resources
                .projection_payload_bytes
                .checked_add(selected.objects.rabitq?.byte_length)?;
        }
        reference?;
        let visible_vectors =
            stored_vectors.checked_sub(self.visibility.total_vector_retractions()?)?;
        resources.raw_vector_bytes = u64::try_from(stored_vectors)
            .ok()?
            .checked_mul(u64::try_from(dimension).ok()?)?
            .checked_mul(size_of::<f32>() as u64)?;
        for run in &self.manifest.mutation_runs {
            resources.projection_payload_bytes =
                resources.projection_payload_bytes.checked_add(run.len)?;
        }
        resources.build_write_amplification_per_million = resources
            .projection_payload_bytes
            .saturating_mul(1_000_000)
            .checked_div(resources.raw_vector_bytes.max(1))?;
        let mut digest = DigestWriter(IntegrityHasher::new());
        digest.0.update(b"hawdb-diskann-qualified-closure-v1\0");
        // The validated manifest binds every content/metadata/vector reference,
        // graph source digest and mutation run, including retraction-only heads.
        serde_json::to_writer(&mut digest, &self.manifest).ok()?;
        let digest = digest.0.finish();
        let identity = VectorProjectionQualificationIdentity {
            projection_generation: self.manifest.generation,
            source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            document_count: visible_vectors,
            source_digest: u64::from_le_bytes(digest.sha256.as_bytes()[..8].try_into().ok()?),
            payload_bytes: resources.projection_payload_bytes,
            payload_checksum: digest.crc32c.get(),
            format_version: 2,
            algorithm: "diskann".into(),
            bit_width: 0,
            dimension,
            transform_seed: 0,
            embedding_model: self.manifest.embedding_model.clone(),
            embedding_version: self.manifest.embedding_version.clone(),
            file_backed: true,
        };
        Some((identity, resources))
    }
}
