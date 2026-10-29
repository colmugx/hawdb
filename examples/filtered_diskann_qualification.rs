// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual synthetic graph/filter qualification over the public library path.

use hawdb::{
    SearchDocument, SearchEmbeddingManifest, SearchMode, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreOutput, SearchOutOfCoreReader,
    SearchQueryOptions, VectorIndexBackend, VectorIndexLsg,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Instant;

#[path = "filtered_diskann_qualification/canonical.rs"]
mod canonical;

const MODEL: &str = "hawdb-filtered-diskann-synthetic";
const VERSION: &str = "clustered-xorshift64star-v1";
const SEED: u64 = 0x94d0_49bb_1331_11eb;
const TOP_K: usize = 10;

fn vector(id: u64, dimension: usize, cluster: usize) -> Vec<f32> {
    let mut state = SEED ^ id.wrapping_mul(0xd6e8_feb8_6659_fd93);
    let spread = if id.is_multiple_of(7) { 0.15 } else { 0.03 };
    let mut output = Vec::with_capacity(dimension);
    for _ in 0..dimension {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let unit = (state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 40) as f32 / 8_388_608.0 - 1.0;
        output.push(unit * spread);
    }
    output[0] += 1.0;
    let angle = cluster as f32 * std::f32::consts::TAU / 100.0;
    output[1] += 0.4 * angle.cos();
    output[2] += 0.4 * angle.sin();
    output
}

fn build(
    path: &Path,
    count: usize,
    dimension: usize,
    memory: usize,
    lsg: bool,
) -> Result<(), Box<dyn Error>> {
    if !(1_000..=100_000).contains(&count)
        || !(3..=1024).contains(&dimension)
        || !((8 << 20)..=(512 << 20)).contains(&memory)
    {
        return Err("fixture requires 1000..=100000 documents, 3..=1024 dimensions and 8..=512 MiB build memory".into());
    }
    let mut backend = VectorIndexBackend::default();
    let VectorIndexBackend::DiskAnn { lsg: settings, .. } = &mut backend else {
        return Err("the default vector engine is not DiskANN".into());
    };
    *settings = lsg.then_some(VectorIndexLsg {
        sample_size: 256,
        neighbors: 10,
        exponent_milli: 1000,
    });
    // Refuse any pre-existing path, including an empty or partially built fixture.
    std::fs::create_dir(path)?;
    let started = Instant::now();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        path,
        SearchOutOfCoreGenerationBuildOptions {
            vector_backend: Some(backend),
            rabitq_build_memory_bytes: NonZeroUsize::new(memory)
                .ok_or("build memory must be positive")?,
            source_graph_commit_epoch: Some(1),
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: MODEL.into(),
                version: Some(VERSION.into()),
                dimension,
            }),
            ..Default::default()
        },
    )?;
    for id in 0..count {
        let cluster = id % 100;
        writer.push(SearchDocument {
            id: format!("memory:{id:016x}"),
            title: String::new(),
            content: String::new(),
            embedding: Some(vector(id as u64, dimension, cluster)),
            metadata: BTreeMap::from([
                ("fine_cluster".into(), cluster.to_string()),
                ("coarse_cluster".into(), (cluster / 10).to_string()),
            ]),
        })?;
    }
    let report = writer.finish()?;
    println!(
        "{}",
        json!({
            "phase": "build", "protocol": VERSION, "seed": SEED,
            "documents": count, "dimension": dimension, "lsg": lsg,
            "configured_build_bytes": memory, "elapsed_ns": started.elapsed().as_nanos(),
            "report": report,
        })
    );
    Ok(())
}

fn query(
    reader: &SearchOutOfCoreReader,
    embedding: &[f32],
    filters: BTreeMap<String, String>,
    exact: bool,
) -> hawdb::Result<SearchOutOfCoreOutput> {
    let options = SearchQueryOptions {
        limit: TOP_K,
        offset: 0,
        rank_window: Some(128),
        fusion_weights: Default::default(),
        metadata_filters: filters,
        policy_epoch: None,
    };
    if exact {
        reader.search_with_options_exact("", Some(embedding), SearchMode::Vector, options)
    } else {
        reader.search_with_options("", Some(embedding), SearchMode::Vector, options)
    }
}

fn qualify(path: &Path, samples: usize) -> Result<(), Box<dyn Error>> {
    if !(1..=128).contains(&samples) {
        return Err("sample count must be in 1..=128".into());
    }
    let opened = Instant::now();
    let reader = SearchOutOfCoreReader::open(path)?;
    let open_ns = opened.elapsed().as_nanos();
    let manifest = reader
        .embedding_manifest()
        .ok_or("fixture has no embedding manifest")?;
    if manifest.model != MODEL || manifest.version.as_deref() != Some(VERSION) {
        return Err("projection is not a matching synthetic qualification fixture".into());
    }
    let identity = reader
        .vector_projection_qualification_identity()
        .ok_or("fixture has no vector identity")?;
    if identity.algorithm != "diskann" || identity.source_graph_commit_epoch != Some(1) {
        return Err("fixture must contain a source-bound DiskANN graph".into());
    }
    println!(
        "{}",
        json!({
            "phase": "open", "protocol": VERSION, "seed": SEED, "open_ns": open_ns,
            "documents": reader.document_count(), "dimension": manifest.dimension,
            "identity": identity.json(), "resource_evidence": reader.vector_projection_resource_evidence().map(|value| value.json()),
            "query_source": "held_out_synthetic_vectors", "production_queries": false,
        })
    );
    let mut totals: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for sample in 0..samples {
        let cluster = sample * 37 % 100;
        let embedding = vector(u64::MAX - sample as u64, manifest.dimension, cluster);
        for (name, field, group) in [
            ("unfiltered", "", 0),
            ("correlated_10_percent", "coarse_cluster", cluster / 10),
            (
                "anticorrelated_10_percent",
                "coarse_cluster",
                (cluster / 10 + 5) % 10,
            ),
            ("correlated_1_percent", "fine_cluster", cluster),
            (
                "anticorrelated_1_percent",
                "fine_cluster",
                (cluster + 50) % 100,
            ),
        ] {
            let filters = if field.is_empty() {
                BTreeMap::new()
            } else {
                BTreeMap::from([(field.to_owned(), group.to_string())])
            };
            let started = Instant::now();
            let exact = query(&reader, &embedding, filters.clone(), true)?;
            let exact_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            let actual = query(&reader, &embedding, filters, false)?;
            let ann_ns = started.elapsed().as_nanos();
            let expected: BTreeSet<_> = exact
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect();
            if expected.is_empty() {
                return Err("empty ground truth cannot qualify recall".into());
            }
            let actual_ids: BTreeSet<_> = actual
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect();
            if actual_ids.len() != actual.result.hits.len()
                || expected.len() != exact.result.hits.len()
                || actual_ids.len() > TOP_K
                || expected.len() > TOP_K
            {
                return Err("query returned duplicate IDs or exceeded top-k".into());
            }
            let overlap = actual_ids.intersection(&expected).count();
            let recall = overlap * 1_000_000 / expected.len();
            let total = totals.entry(name).or_default();
            total.0 += expected.len();
            total.1 += overlap;
            for hit in &actual.result.hits {
                let id = usize::from_str_radix(
                    hit.id
                        .strip_prefix("memory:")
                        .ok_or("invalid fixture hit ID")?,
                    16,
                )?;
                let fine = id % 100;
                if (field == "fine_cluster" && fine != group)
                    || (field == "coarse_cluster" && fine / 10 != group)
                {
                    return Err("graph query returned an ineligible fixture document".into());
                }
            }
            let metrics = actual
                .result
                .retrievers
                .iter()
                .find(|value| value.name == "vector")
                .ok_or("missing vector retriever report")?;
            if metrics.backend != "hawdb_diskann_out_of_core_candidate_graph"
                || !metrics.fallback_reason_codes.is_empty()
            {
                return Err("qualification did not execute the requested graph backend".into());
            }
            if metrics.reranked_candidate_count > 128 {
                return Err("default graph exceeded the requested rerank window".into());
            }
            println!(
                "{}",
                json!({
                    "phase": "query", "case": name, "sample": sample,
                    "recall_per_million": recall, "exact_hits": expected.len(), "overlap": overlap,
                    "exact_ns": exact_ns, "ann_ns": ann_ns, "backend": metrics.backend,
                    "scored_vectors": metrics.candidate_scan_scored_document_count,
                "reranked_vectors": metrics.reranked_candidate_count,
                "rejected_vertices": metrics.candidate_scan_filtered_document_count,
                "expanded_vertices": metrics.candidate_scan_scanned_block_count,
                "filter_selectivity_per_million": metrics.filter_selectivity_per_million,
                    "candidate_payload_bytes": metrics.candidate_scan_payload_bytes_read,
                    "raw_rerank_bytes": metrics.raw_vector_bytes_read,
                    "admitted_working_bytes": metrics.candidate_scan_admitted_working_bytes,
                })
            );
        }
    }
    let mut failures = 0usize;
    let summaries: Vec<_> = totals
        .into_iter()
        .map(|(name, (expected, overlap))| {
            let recall = overlap * 1_000_000 / expected;
            failures += usize::from(recall < 950_000);
            json!({"case": name, "exact_hits": expected, "overlap": overlap, "recall_per_million": recall})
        })
        .collect();
    println!(
        "{}",
        json!({"phase": "complete", "samples": samples, "cases": samples * 5, "recall_failures": failures, "summaries": summaries})
    );
    if failures > 0 {
        return Err("one or more filter families missed the mean recall threshold".into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.first().map(String::as_str) {
        Some("build") if arguments.len() == 6 => {
            let memory = arguments[4].parse::<usize>()?.checked_mul(1 << 20).ok_or("build memory overflow")?;
            let lsg = match arguments[5].as_str() { "plain" => false, "lsg" => true, _ => return Err("mode must be plain or lsg".into()) };
            build(Path::new(&arguments[1]), arguments[2].parse()?, arguments[3].parse()?, memory, lsg)
        }
        Some("query") if arguments.len() == 3 => qualify(Path::new(&arguments[1]), arguments[2].parse()?),
        Some("canonical") => canonical::run(&arguments[1..]),
        _ => Err("usage: filtered_diskann_qualification build <new-root> <documents> <dimension> <memory-MiB> <plain|lsg> | query <root> <samples> | canonical <new-root> <documents> <dimension> <memory-MiB> <plain|lsg> <mutation-per-mille> <samples>".into()),
    }
}
