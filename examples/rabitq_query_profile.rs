// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Manual profiling fixture for the full file-backed vector query and rerank.

use hawdb::{
    CompressedVectorSearchMode, SearchDocument, SearchEmbeddingManifest, SearchFusionWeights,
    SearchMode, SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreOutput, SearchOutOfCoreReader, SearchQueryOptions, VectorSearchExecutionOptions,
    VectorSearchKernelPreference,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

const DOCUMENTS: usize = 100_000;
const DIMENSION: usize = 384;
const REPETITIONS: usize = 100;
const SEED: u64 = 0x9940_2026_6f91_31c5;

fn vector(id: u64) -> Vec<f32> {
    let mut state = SEED ^ id.wrapping_mul(0xd6e8_feb8_6659_fd93);
    let mut values = (0..DIMENSION)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let bits = state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 40;
            bits as f32 / 8_388_608.0 - 1.0
        })
        .collect::<Vec<_>>();
    let norm = values
        .iter()
        .map(|&value| f64::from(value).powi(2))
        .sum::<f64>()
        .sqrt();
    for value in &mut values {
        *value = (f64::from(*value) / norm) as f32;
    }
    values
}

fn build(path: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir(path)?;
    let options = SearchOutOfCoreGenerationBuildOptions {
        vector_backend: None,
        source_graph_commit_epoch: Some(1),
        embedding_manifest: Some(SearchEmbeddingManifest {
            model: "hawdb-rabitq-profile-xorshift64star".into(),
            version: Some(format!("v1-{SEED:016x}")),
            dimension: DIMENSION,
        }),
        ..SearchOutOfCoreGenerationBuildOptions::default()
    };
    let bit_width = options.rabitq_bit_width.bits();
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, options)?;
    for id in 0..DOCUMENTS {
        writer.push(SearchDocument {
            id: format!("memory:{id:016x}"),
            title: "Synthetic vector profile".into(),
            content: "Fixed synthetic content for query hydration.".into(),
            embedding: Some(vector(id as u64)),
            metadata: BTreeMap::new(),
        })?;
    }
    let report = writer.finish()?;
    assert_eq!(report.document_count, DOCUMENTS);
    assert_eq!(report.vector_document_count, DOCUMENTS);
    assert!(report.rabitq_artifact_bytes > 0);
    println!(
        "{}",
        json!({
            "phase": "built", "documents": DOCUMENTS, "dimension": DIMENSION,
            "bit_width": bit_width, "generator_seed": SEED, "report": report,
        })
    );
    Ok(())
}

fn query(
    reader: &SearchOutOfCoreReader,
    embedding: &[f32],
    kernel: VectorSearchKernelPreference,
) -> hawdb::Result<SearchOutOfCoreOutput> {
    reader.search_with_options_compressed_vector_projection_execution_options(
        "",
        Some(embedding),
        SearchMode::Vector,
        SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: Some(100),
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        },
        CompressedVectorSearchMode::Required,
        VectorSearchExecutionOptions::default().with_kernel(kernel),
    )
}

fn hit_bits(output: &SearchOutOfCoreOutput) -> Vec<(String, u64)> {
    output
        .result
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.vector_score.to_bits()))
        .collect()
}

fn profile(path: &Path) -> Result<(), Box<dyn Error>> {
    let reader = SearchOutOfCoreReader::open(path)?;
    assert_eq!(reader.document_count(), DOCUMENTS);
    let embedding = vector(u64::MAX - 17);
    let mut warm = query(&reader, &embedding, VectorSearchKernelPreference::Scalar)?;
    for _ in 0..2 {
        warm = query(&reader, &embedding, VectorSearchKernelPreference::Scalar)?;
    }
    let expected = hit_bits(&warm);
    assert_eq!(expected.len(), 10);
    let retriever = warm
        .result
        .retrievers
        .iter()
        .find(|r| r.name == "vector")
        .unwrap();
    assert_eq!(
        retriever.backend,
        "hawdb_rabitq_out_of_core_candidate_projection"
    );
    assert_eq!(retriever.candidate_scan_kernel.as_deref(), Some("scalar"));
    // The report sums workers across sequential artifact layers.
    assert!(retriever.candidate_scan_worker_count > 0);
    assert_eq!(retriever.candidate_scan_scored_document_count, DOCUMENTS);
    assert!(retriever.reranked_candidate_count > 0);
    assert!(retriever.raw_vector_bytes_read > 0);
    assert!(retriever.fallback_reason_codes.is_empty());
    println!(
        "{}",
        json!({
            "phase": "ready", "pid": std::process::id(), "repetitions": REPETITIONS,
            "kernel": retriever.candidate_scan_kernel, "backend": retriever.backend,
            "configured_max_parallelism": 1,
            "layer_worker_count_sum": retriever.candidate_scan_worker_count,
            "candidate_score_source": retriever.candidate_score_source,
            "final_score_source": retriever.final_score_source,
            "scored_vectors": retriever.candidate_scan_scored_document_count,
            "reranked_candidates": retriever.reranked_candidate_count,
            "raw_vector_bytes_read": retriever.raw_vector_bytes_read,
            "admitted_working_bytes": retriever.candidate_scan_admitted_working_bytes,
            "hits": expected,
        })
    );
    io::stdout().flush()?;
    // Attach the profiler after warmup; parsing this command is outside query timers.
    let mut command = String::new();
    io::stdin().read_line(&mut command)?;
    if command.trim() != "run" {
        return Err("send 'run' on stdin after attaching the profiler".into());
    }
    let mut elapsed = Vec::with_capacity(REPETITIONS);
    for _ in 0..REPETITIONS {
        let started = Instant::now();
        let result = query(&reader, &embedding, VectorSearchKernelPreference::Scalar)?;
        elapsed.push(started.elapsed().as_nanos());
        assert_eq!(hit_bits(&result), expected);
    }
    println!("{}", json!({"phase": "complete", "elapsed_ns": elapsed}));
    Ok(())
}

fn compare(path: &Path) -> Result<(), Box<dyn Error>> {
    const SAMPLES: usize = 31;
    let reader = SearchOutOfCoreReader::open(path)?;
    assert_eq!(reader.document_count(), DOCUMENTS);
    let (simd, kernel_name) = if cfg!(target_arch = "aarch64") {
        (VectorSearchKernelPreference::Neon, "neon")
    } else {
        (VectorSearchKernelPreference::Avx2, "avx2")
    };
    let embeddings = [17, 29, 41].map(|offset| vector(u64::MAX - offset));
    let mut expected = Vec::new();
    for embedding in &embeddings {
        let scalar = query(&reader, embedding, VectorSearchKernelPreference::Scalar)?;
        let selected = query(&reader, embedding, simd)?;
        assert_eq!(hit_bits(&scalar), hit_bits(&selected));
        assert_eq!(scalar.result.hits.len(), 10);
        let mut reports = Vec::new();
        for (output, kernel) in [(&scalar, "scalar"), (&selected, kernel_name)] {
            let report = output
                .result
                .retrievers
                .iter()
                .find(|r| r.name == "vector")
                .unwrap();
            assert_eq!(
                report.backend,
                "hawdb_rabitq_out_of_core_candidate_projection"
            );
            assert_eq!(report.candidate_scan_kernel.as_deref(), Some(kernel));
            assert_eq!(report.candidate_scan_scored_document_count, DOCUMENTS);
            assert!(report.reranked_candidate_count > 0);
            assert!(report.raw_vector_bytes_read > 0);
            assert!(report.fallback_reason_codes.is_empty());
            reports.push(json!({
                "scored_vectors": report.candidate_scan_scored_document_count,
                "reranked_candidates": report.reranked_candidate_count,
                "raw_vector_bytes_read": report.raw_vector_bytes_read,
                "admitted_working_bytes": report.candidate_scan_admitted_working_bytes,
                "layer_worker_count_sum": report.candidate_scan_worker_count,
            }));
        }
        assert_eq!(reports[0], reports[1]);
        println!(
            "{}",
            json!({"phase": "comparison_ready", "report": reports[0]})
        );
        expected.push(hit_bits(&scalar));
    }
    let mut elapsed = [Vec::new(), Vec::new()];
    for sample in 0..SAMPLES {
        let query_index = sample % embeddings.len();
        for index in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
            let kernel = [VectorSearchKernelPreference::Scalar, simd][index];
            let started = Instant::now();
            let result = query(&reader, &embeddings[query_index], kernel)?;
            elapsed[index].push(started.elapsed().as_nanos());
            assert_eq!(hit_bits(&result), expected[query_index]);
        }
    }
    println!(
        "{}",
        json!({
            "phase": "comparison_complete", "documents": DOCUMENTS, "dimension": DIMENSION,
            "generator_seed": SEED, "samples_per_kernel": SAMPLES, "queries": embeddings.len(),
            "configured_max_parallelism": 1, "simd_kernel": kernel_name,
            "scalar_elapsed_ns": elapsed[0], "simd_elapsed_ns": elapsed[1],
        })
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    if cfg!(debug_assertions) {
        return Err("run the profiling fixture with --release".into());
    }
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() != 2 {
        return Err("usage: rabitq_query_profile <build|profile|compare> <fixture-path>".into());
    }
    match arguments[0].to_str() {
        Some("build") => build(Path::new(&arguments[1])),
        Some("profile") => profile(Path::new(&arguments[1])),
        Some("compare") => compare(Path::new(&arguments[1])),
        _ => Err("mode must be build, profile or compare".into()),
    }
}
