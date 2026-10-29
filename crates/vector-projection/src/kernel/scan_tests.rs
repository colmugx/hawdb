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

use super::*;
use crate::{
    FileProjection, InMemoryProjection, ProjectionBuildConfig, ProjectionBuilder,
    ProjectionIdentity, ProjectionSearchOptions, ProjectionSearchOutput, ProjectionWriter,
};
use std::{
    fs,
    hint::black_box,
    num::NonZeroUsize,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

struct Fixture {
    memory: InMemoryProjection,
    file: FileProjection,
    _directory: TestDirectory,
}

impl Fixture {
    fn new(dimension: usize, width: RaBitQBitWidth, rows: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-simd-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
            .with_bit_width(width)
            .with_segment_rows(rows.div_ceil(4));
        let mut builder = ProjectionBuilder::new(config.clone()).unwrap();
        let mut writer = ProjectionWriter::create(root.join("projection.hawdb"), config).unwrap();
        for id in 0..rows as u64 {
            // Duplicate vectors exercise score ties and document-ID ordering.
            let vector = vector(id / 2, dimension);
            builder.push(id, &vector).unwrap();
            writer.push(id, &vector).unwrap();
        }
        Self {
            memory: builder.finish().unwrap(),
            file: writer.finish().unwrap(),
            _directory: TestDirectory(root),
        }
    }

    fn search(
        &self,
        file: bool,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> ProjectionSearchOutput {
        if file {
            self.file.search(query, top_k, options)
        } else {
            self.memory.search(query, top_k, options)
        }
        .unwrap()
    }
}

// Fields drop in declaration order: the mapping closes before directory removal.
struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn vector(id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|index| {
            let mixed = id
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add((index as u64).wrapping_mul(0x85eb_ca6b));
            ((mixed % 2001) as f32 - 1000.0) / 1000.0
        })
        .collect()
}

fn assert_parity(scalar: &ProjectionSearchOutput, selected: &ProjectionSearchOutput) {
    assert_eq!(scalar.hits.len(), selected.hits.len());
    for (expected, actual) in scalar.hits.iter().zip(&selected.hits) {
        assert_eq!(expected.id, actual.id);
        assert_eq!(expected.score.to_bits(), actual.score.to_bits());
    }
    let mut report = selected.report.clone();
    report.kernel = ScanKernel::Scalar;
    assert_eq!(scalar.report, report);
}

#[test]
fn memory_and_file_scans_preserve_scores_filters_and_parallel_merges() {
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for dimension in [7, 8, 65, 384, 768, 769, 1536] {
            let fixture = Fixture::new(dimension, width, 65);
            let query = vector(19, dimension);
            let sparse = [0, 31, 32, 64, 100];
            let dense = (0..65).filter(|id| id % 3 != 0).collect::<Vec<_>>();
            for allowed in [None, Some(&[][..]), Some(&sparse), Some(dense.as_slice())] {
                for workers in [1, 4] {
                    let mut options = ProjectionSearchOptions::new()
                        .with_max_parallelism(NonZeroUsize::new(workers).unwrap());
                    if let Some(ids) = allowed {
                        options = options.with_allowed_ids(ids);
                    }
                    for file in [false, true] {
                        for top_k in [1, 10, 100] {
                            let scalar = fixture.search(
                                file,
                                &query,
                                top_k,
                                options.with_kernel(KernelPreference::Scalar),
                            );
                            let selected = fixture.search(
                                file,
                                &query,
                                top_k,
                                options.with_kernel(
                                    native_simd_preference().unwrap_or(KernelPreference::Scalar),
                                ),
                            );
                            assert_parity(&scalar, &selected);
                        }
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "local release benchmark; requires native AVX2 or AArch64 NEON"]
fn compare_simd_projection_scans() {
    let preference = native_simd_preference().expect("a native SIMD kernel is required");
    let kernel = select_kernel(preference).unwrap();
    super::parity_tests::verify_packed_code_parity();
    const ROWS: usize = 4096;
    const SAMPLES: usize = 15;
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for dimension in [384, 768, 769, 1536] {
            let fixture = Fixture::new(dimension, width, ROWS);
            let queries = [
                vector(17, dimension),
                vector(29, dimension),
                vector(41, dimension),
            ];
            let query_checksum = crc32fast::hash(
                &queries
                    .iter()
                    .flatten()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
            );
            for density in [1, 50, 100] {
                let allowed = (0..ROWS as u64)
                    .filter(|id| id % 100 < density)
                    .collect::<Vec<_>>();
                for workers in [1, 4] {
                    let mut options = ProjectionSearchOptions::new()
                        .with_max_parallelism(NonZeroUsize::new(workers).unwrap());
                    if density != 100 {
                        options = options.with_allowed_ids(&allowed);
                    }
                    for file in [false, true] {
                        let baseline = fixture.search(
                            file,
                            &queries[0],
                            10,
                            options.with_kernel(KernelPreference::Scalar),
                        );
                        let warmup =
                            fixture.search(file, &queries[0], 10, options.with_kernel(preference));
                        assert_eq!(warmup.report.kernel, kernel);
                        assert_parity(&baseline, &warmup);
                        let mut times = [Vec::new(), Vec::new()];
                        for sample in 0..SAMPLES {
                            let query = &queries[sample % queries.len()];
                            let reference = fixture.search(
                                file,
                                query,
                                10,
                                options.with_kernel(KernelPreference::Scalar),
                            );
                            for index in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                                let preference = [KernelPreference::Scalar, preference][index];
                                let started = Instant::now();
                                let output = fixture.search(
                                    file,
                                    black_box(query),
                                    10,
                                    options.with_kernel(preference),
                                );
                                times[index].push(started.elapsed().as_secs_f64() * 1e6);
                                assert_parity(&reference, &output);
                                black_box(output);
                            }
                        }
                        for samples in &mut times {
                            samples.sort_by(f64::total_cmp);
                        }
                        let scalar_us = times[0][SAMPLES / 2];
                        let simd_us = times[1][SAMPLES / 2];
                        println!(
                            "simd_scan {}",
                            serde_json::json!({
                                "bits": width.bits(), "dimension": dimension, "rows": ROWS,
                                "kernel": kernel.as_str(),
                                "file": file, "density_percent": density, "requested_workers": workers,
                                "workers": warmup.report.worker_count, "samples": SAMPLES,
                                "scored_rows": warmup.report.scored_document_count,
                                "working_bytes": warmup.report.admitted_working_bytes,
                                "payload_checksum": fixture.file.manifest().payload_checksum,
                                "query_checksum": query_checksum,
                                "scalar_us": scalar_us, "simd_us": simd_us, "speedup": scalar_us / simd_us,
                            })
                        );
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "local deadline-response benchmark; requires native SIMD"]
fn measure_simd_scan_cancellation() {
    use hawdb_core::{RuntimeCancellationReason, RuntimeTaskContext};
    use std::time::Duration;

    let preference = native_simd_preference().expect("a native SIMD kernel is required");
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let fixture = Fixture::new(1536, width, 8192);
        let query = vector(17, 1536);
        for file in [false, true] {
            for workers in [1, 4] {
                let workers = NonZeroUsize::new(workers).unwrap();
                for kernel in [KernelPreference::Scalar, preference] {
                    let options = ProjectionSearchOptions::new()
                        .with_kernel(kernel)
                        .with_max_parallelism(workers);
                    black_box(fixture.search(file, &query, 10, options));
                    let mut overshoots = Vec::new();
                    let mut completed = 0;
                    for _ in 0..15 {
                        let context = RuntimeTaskContext::with_timeout(Duration::from_millis(1))
                            .with_admitted_parallelism(workers);
                        let options = options.with_task_context(&context);
                        let result = if file {
                            fixture.file.search(&query, 10, options)
                        } else {
                            fixture.memory.search(&query, 10, options)
                        };
                        let overshoot = Instant::now()
                            .saturating_duration_since(context.deadline().unwrap())
                            .as_secs_f64()
                            * 1e6;
                        match result {
                            Err(ProjectionError::Cancelled(
                                RuntimeCancellationReason::DeadlineExceeded,
                            )) => {
                                overshoots.push(overshoot);
                            }
                            Ok(_) => completed += 1,
                            Err(error) => panic!("unexpected cancellation result: {error}"),
                        }
                    }
                    overshoots.sort_by(f64::total_cmp);
                    println!(
                        "simd_cancellation {}",
                        serde_json::json!({
                            "bits": width.bits(), "dimension": 1536, "rows": 8192,
                            "file": file, "workers": workers.get(),
                            "kernel": select_kernel(kernel).unwrap().as_str(),
                            "deadline_us": 1000, "cancelled": overshoots.len(),
                            "completed": completed,
                            "median_overshoot_us": overshoots.get(overshoots.len() / 2),
                            "max_overshoot_us": overshoots.last(),
                        })
                    );
                }
            }
        }
    }
}
