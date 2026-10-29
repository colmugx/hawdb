// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::build_memory::{ACCOUNT_METADATA_BYTES, MAP_ENTRY_BYTES};

fn strings(values: &[String]) -> usize {
    values
        .iter()
        .fold(std::mem::size_of_val(values), |bytes, value| {
            bytes.saturating_add(value.capacity())
        })
}

pub(super) fn generation(generation: &ReadGeneration) -> usize {
    let index = &generation.index;
    let mut bytes = index
        .documents
        .retained_capacity_bytes()
        .saturating_add(std::mem::size_of::<ReadGeneration>())
        .saturating_add(ACCOUNT_METADATA_BYTES);
    bytes = bytes.saturating_add(index.path.as_ref().map_or(0, PathBuf::capacity));
    if let Some(binding) = &index.vector_index_descriptor {
        bytes = bytes.saturating_add(binding.retained_capacity_bytes());
    }
    if let Some(manifest) = &index.embedding_manifest {
        bytes = bytes
            .saturating_add(manifest.model.capacity())
            .saturating_add(manifest.version.as_ref().map_or(0, String::capacity));
    }
    for (name, reasons) in index
        .marker_lines
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
    {
        bytes = bytes
            .saturating_add(name.capacity())
            .saturating_add(MAP_ENTRY_BYTES)
            .saturating_add(
                reasons
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(strings(reasons));
    }
    let freshness = &generation.freshness;
    bytes = bytes
        .saturating_add(std::mem::size_of::<SearchProjectionFreshness>())
        .saturating_add(strings(&freshness.full_reindex_reasons))
        .saturating_add(strings(&freshness.metadata_repair_reasons))
        .saturating_add(
            freshness
                .embedding_model
                .as_ref()
                .map_or(0, String::capacity),
        )
        .saturating_add(
            freshness
                .embedding_version
                .as_ref()
                .map_or(0, String::capacity),
        );
    let analyzer = &index.analyzer_lexicon;
    bytes = bytes.saturating_add(
        analyzer
            .alias_rules
            .capacity()
            .saturating_mul(std::mem::size_of::<SearchAnalyzerAliasRule>()),
    );
    for rule in &analyzer.alias_rules {
        bytes = bytes
            .saturating_add(
                rule.inputs
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(
                rule.aliases
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(strings(&rule.inputs))
            .saturating_add(strings(&rule.aliases));
    }
    for word in &analyzer.stopwords {
        bytes = bytes
            .saturating_add(MAP_ENTRY_BYTES)
            .saturating_add(word.capacity());
    }
    if let Some(descriptor) = &index.segment_descriptor {
        bytes = bytes.saturating_add(
            descriptor
                .segments
                .capacity()
                .saturating_mul(std::mem::size_of::<SearchSegmentDescriptorEntry>()),
        );
        for segment in &descriptor.segments {
            bytes = bytes
                .saturating_add(segment.first_document_id.capacity())
                .saturating_add(segment.last_document_id.capacity());
            for (name, summary) in &segment.metadata {
                bytes = bytes
                    .saturating_add(MAP_ENTRY_BYTES)
                    .saturating_add(name.capacity());
                for value in &summary.values {
                    bytes = bytes
                        .saturating_add(MAP_ENTRY_BYTES)
                        .saturating_add(value.capacity());
                }
            }
        }
    }
    if let Some((reader, delta)) = index.lexical_snapshot() {
        bytes = bytes
            .saturating_add(reader.retained_capacity_bytes())
            .saturating_add(delta.retained_capacity_bytes());
    }
    if let Some(reader) = &index.segment_payload_reader {
        bytes = bytes.saturating_add(reader.retained_capacity_bytes());
    }
    #[cfg(feature = "vector-search")]
    {
        bytes = bytes.saturating_add(index.resident_diskann.retained_capacity_bound(index));
    }
    #[cfg(feature = "vector-search")]
    if index.path.is_none() {
        // Fixed before the first query, including the lazy build's transient
        // buffers. This charge does not grow after a lease has been admitted.
        bytes = bytes.saturating_add(RaBitQCandidateProjection::in_memory_capacity_bound(
            &index.documents,
            index.rabitq_projection_identity(0),
            index.rabitq_build_options,
        ));
    } else if let Some(projection) = index.rabitq_projection() {
        bytes = bytes.saturating_add(projection.retained_capacity_bytes());
    }
    bytes
}
