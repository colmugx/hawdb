// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual qualification through canonical SQL, Consumer publication and MVCC.

use super::{vector, SEED, TOP_K, VERSION};
use hawdb::{
    Database, DatabaseConfig, HawDBError, IoConcurrencyBudget, QueryStreamOptions, RuntimeGovernor,
    RuntimeGovernorConfig, RuntimeMemoryReservation, RuntimePermit, RuntimeTaskContext,
    RuntimeWorkRequest, SearchProjectionConsumer, SearchProjectionConsumerError,
    SearchProjectionConsumerId, SearchProjectionConsumerOptions, StorageResidencyMode, Value,
    Vector, VectorDistance, VectorIndexBackend, VectorIndexBuildError, VectorIndexBuildOptions,
    VectorIndexField, VectorIndexLanguageBudget, VectorIndexLsg, VectorIndexReadError,
    VectorIndexReadTransaction, VectorIndexSqlRequest, VectorIndexUpdateReport,
};
use hawdb_integrity::IntegrityHasher;
use hawdb_qos::RuntimeAdmissionCode;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::Read;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Hits = Vec<(usize, f64)>;

#[derive(Clone, Copy)]
struct Case<'a> {
    sample: usize,
    field: &'a str,
    group: usize,
}

struct Fixture {
    count: usize,
    dimension: usize,
    changes: Vec<usize>,
}

impl Fixture {
    fn embedding(&self, id: usize, changed: bool) -> Option<Vec<f32>> {
        if id >= self.count {
            return (changed && id < self.count + self.changes.len() / 4)
                .then(|| vector(id as u64, self.dimension, id % 100));
        }
        let operation = changed
            .then(|| self.changes.iter().position(|key| *key == id))
            .flatten();
        match operation.map(|index| index % 4) {
            Some(1 | 2) => None,
            Some(0) => Some(vector(
                id as u64 ^ (1 << 63),
                self.dimension,
                (id % 100 + 23) % 100,
            )),
            _ => Some(vector(id as u64, self.dimension, id % 100)),
        }
    }

    fn eligible(&self, changed: bool, field: &str, group: usize) -> usize {
        let matches = |id: usize| match field {
            "fine_cluster" => id % 100 == group,
            "coarse_cluster" => id % 100 / 10 == group,
            _ => true,
        };
        let removed: BTreeSet<_> = self
            .changes
            .iter()
            .enumerate()
            .filter(|(index, _)| changed && matches!(index % 4, 1 | 2))
            .map(|(_, id)| *id)
            .collect();
        let end = self.count + usize::from(changed) * self.changes.len() / 4;
        (0..end)
            .filter(|id| matches(*id) && !removed.contains(id))
            .count()
    }
}

fn query(
    read: &VectorIndexReadTransaction,
    permit: &RuntimePermit,
    fixture: &Fixture,
    changed: bool,
    case: Case<'_>,
    exact: bool,
    phase: &str,
) -> Result<Hits> {
    let Case {
        sample,
        field,
        group,
    } = case;
    let cluster = sample * 37 % 100;
    let coordinates = vector(u64::MAX - sample as u64, fixture.dimension, cluster);
    let mode = if exact {
        "exact"
    } else if field.is_empty() {
        "ann_window"
    } else {
        "filtered_ann"
    };
    let candidates = if exact { "" } else { ", candidates => 128" };
    let work = if exact || !field.is_empty() {
        format!(", max_work => {}", fixture.count * 100)
    } else {
        String::new()
    };
    let predicate = if field.is_empty() {
        String::new()
    } else {
        format!(" WHERE v.{field} = $2")
    };
    let sql = format!(
        "SELECT v.id AS id, distance FROM vector_index_search('canonical', $1, mode => '{mode}'{candidates}, top_k => {TOP_K}{work}) AS v{predicate} ORDER BY distance, id"
    );
    let mut parameters = vec![Value::Vector(Vector::new(&coordinates)?)];
    if !field.is_empty() {
        parameters.push(Value::Int(group as i64));
    }
    let mut hits = Vec::new();
    let started = Instant::now();
    let report = read.query_sql_streaming(
        permit,
        VectorIndexLanguageBudget {
            index_working_bytes: NonZeroUsize::new(64 << 20).unwrap(),
            index_result_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        },
        VectorIndexSqlRequest {
            sql: &sql,
            parameters: &parameters,
            output_limits: QueryStreamOptions {
                max_rows: Some(TOP_K),
                max_payload_bytes: Some(1 << 20),
            },
            task: Default::default(),
        },
        |row| {
            let (Value::Int(id), Value::Float(distance)) = (&row["id"], &row["distance"]) else {
                return Err(hawdb::HawDBError::Execution(
                    "invalid qualification row".into(),
                ));
            };
            let id = usize::try_from(*id)
                .map_err(|_| hawdb::HawDBError::Execution("negative fixture ID".into()))?;
            hits.push((id, *distance));
            Ok(())
        },
    )?;
    let elapsed = started.elapsed();
    let index = report.index();
    let eligible = fixture.eligible(changed, field, group);
    if index.backend != if exact { "exact" } else { "diskann" }
        || index.source_epoch != read.source_epoch()
        || index.generation != read.generation()
        || hits.len() != TOP_K.min(eligible)
        || hits.iter().map(|hit| hit.0).collect::<BTreeSet<_>>().len() != hits.len()
        || (!exact && index.reranked_candidate_count > 128)
    {
        return Err(format!("invalid canonical query report: {index:?}").into());
    }
    if let Some(domain) = &index.eligibility
        && (!domain.complete || domain.eligible_vectors != eligible)
    {
        return Err(
            format!("canonical eligibility mismatch: {domain:?}, expected {eligible}").into(),
        );
    }
    for (id, distance) in &hits {
        let embedding = fixture
            .embedding(*id, changed)
            .ok_or("returned a deleted/NULL vector")?;
        if (field == "fine_cluster" && id % 100 != group)
            || (field == "coarse_cluster" && id % 100 / 10 != group)
        {
            return Err("returned an ineligible vector".into());
        }
        let expected = VectorDistance::Cosine.distance_slices_with_work(
            &coordinates,
            &embedding,
            || Ok(()),
        )?;
        if (distance - expected).abs() > 1e-9 {
            return Err(format!("raw distance mismatch for {id}: {distance} vs {expected}").into());
        }
    }
    let metrics = report.index_metrics();
    println!(
        "{}",
        json!({
            "phase": phase, "event": "query", "sample": sample,
            "filter_field": field, "filter_group": group, "mode": mode,
            "elapsed_ns": elapsed.as_nanos(), "hits": hits,
            "source_epoch": index.source_epoch, "generation": index.generation,
            "schema_digest": index.schema_digest, "backend": index.backend,
            "generated_candidates": index.generated_candidate_count,
            "reranked_candidates": index.reranked_candidate_count,
            "eligible_vectors": index.eligibility.as_ref().map(|value| value.eligible_vectors),
            "eligibility_work": index.eligibility.as_ref().map(|value| value.work_units),
            "scored_eligible_vectors": index.eligibility.as_ref().map(|value| value.scored_vectors),
            "index_retained_capacity_bytes": report.index_retained_capacity_bytes(),
            "index_logical_vector_bytes_read": metrics.vector_bytes_read,
            "index_logical_segment_bytes_read": metrics.segment_bytes_read,
            "candidate_spill_bytes": metrics.candidate_spill_bytes,
            "canonical_rows_visited": report.row_execution.rows_visited,
            "canonical_logical_bytes": report.row_execution.logical_bytes,
            "eligibility_rows_visited": report.eligibility_row_execution.as_ref().map(|value| value.rows_visited),
        })
    );
    Ok(hits)
}

fn qualify(
    read: &VectorIndexReadTransaction,
    permit: &RuntimePermit,
    fixture: &Fixture,
    changed: bool,
    samples: usize,
    phase: &str,
    baseline: Option<&[Hits]>,
) -> Result<Vec<Hits>> {
    let mut output = Vec::new();
    let mut totals = BTreeMap::<&str, (usize, usize)>::new();
    for sample in 0..samples {
        let cluster = sample * 37 % 100;
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
            let case = Case {
                sample,
                field,
                group,
            };
            let actual = query(read, permit, fixture, changed, case, false, phase)?;
            if let Some(baseline) = baseline {
                if baseline.get(output.len()) != Some(&actual) {
                    return Err("retained old MVCC domain changed its ordered results".into());
                }
            } else {
                let exact = query(read, permit, fixture, changed, case, true, phase)?;
                let expected: BTreeSet<_> = exact.iter().map(|hit| hit.0).collect();
                let overlap = actual
                    .iter()
                    .filter(|hit| expected.contains(&hit.0))
                    .count();
                let total = totals.entry(name).or_default();
                total.0 += expected.len();
                total.1 += overlap;
                println!(
                    "{}",
                    json!({"phase": phase, "event": "recall", "case": name,
                    "sample": sample, "overlap": overlap, "exact_hits": expected.len()})
                );
            }
            output.push(actual);
        }
    }
    if totals
        .values()
        .any(|(expected, overlap)| overlap * 100 < expected * 95)
    {
        return Err(format!("canonical mean recall below 95%: {totals:?}").into());
    }
    println!(
        "{}",
        json!({"phase": phase, "event": "qualified", "cases": output.len(), "recall_totals": totals})
    );
    Ok(output)
}

fn resources(
    db: &Database,
    governor: &RuntimeGovernor,
    phase: &str,
    started: Instant,
) -> Result<()> {
    let snapshot = governor.snapshot();
    let descriptors = db
        .file_descriptor_metrics()
        .ok_or("missing native FD metrics")?;
    println!(
        "{}",
        json!({"phase": phase, "event": "resources",
            "elapsed_ns": started.elapsed().as_nanos(),
            "source_files": db.storage_artifact_file_sizes()?,
            "residency": format!("{:?}", db.storage_residency_report()?),
            "fd_limit": descriptors.effective_limit, "fd_open": descriptors.open,
            "fd_high_water": descriptors.high_water, "fd_budget_rejections": descriptors.budget_rejections,
            "governor_admitted_bytes": snapshot.admitted_memory_bytes,
            "governor_admission_rejections": snapshot.admission_rejections,
            "governor_limits": format!("{:?}", snapshot.limits),
            "host_resources": format!("{:?}", snapshot.resources),
        })
    );
    Ok(())
}

fn graphs(root: &Path) -> Result<BTreeMap<PathBuf, String>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            files.extend(graphs(&entry.path())?);
        } else if entry
            .file_name()
            .to_string_lossy()
            .starts_with("search_rabitq.")
        {
            let mut file = std::fs::File::open(entry.path())?;
            let mut hash = IntegrityHasher::new();
            let mut buffer = [0; 64 << 10];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hash.update(&buffer[..read]);
            }
            files.insert(entry.path(), hash.finish().sha256.to_string());
        }
    }
    Ok(files)
}

fn assert_reused(root: &Path, original: &BTreeMap<PathBuf, String>) -> Result<()> {
    let current = graphs(root)?;
    for (path, digest) in original {
        if current.get(path) != Some(digest) {
            return Err("retained baseline graph bytes changed".into());
        }
        let mut reused = false;
        for (other, hash) in &current {
            if other == path || hash != digest {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let first = std::fs::metadata(path)?;
                let second = std::fs::metadata(other)?;
                reused |= first.dev() == second.dev() && first.ino() == second.ino();
            }
            #[cfg(not(unix))]
            {
                reused = true;
            }
        }
        if !reused {
            return Err("successor did not retain a baseline graph artifact".into());
        }
    }
    Ok(())
}

fn mutate(db: &mut Database, fixture: &Fixture, restore: bool) -> Result<()> {
    let mut transaction = db.begin_transaction()?;
    for (index, original) in fixture.changes.iter().enumerate() {
        let operation = index % 4;
        let id = if operation == 3 {
            fixture.count + index / 4
        } else {
            *original
        };
        let embedding = if restore {
            fixture.embedding(id, false)
        } else {
            fixture.embedding(id, true)
        };
        if (restore && operation == 3) || (!restore && operation == 1) {
            transaction.query_sql_with_params(
                "DELETE FROM vectors WHERE id = $1",
                &[Value::Int(id as i64)],
            )?;
        } else if (restore && operation == 1) || (!restore && operation == 3) {
            transaction.query_sql_with_params(
                "INSERT INTO vectors (id, coarse_cluster, fine_cluster, embedding) VALUES ($1, $2, $3, $4)",
                &[Value::Int(id as i64), Value::Int((id % 100 / 10) as i64),
                    Value::Int((id % 100) as i64), Value::Vector(Vector::new(&embedding.ok_or("missing inserted embedding")?)?)],
            )?;
        } else {
            transaction.query_sql_with_params(
                "UPDATE vectors SET embedding = $1 WHERE id = $2",
                &[
                    embedding
                        .map(|value| Vector::new(&value).map(Value::Vector))
                        .transpose()?
                        .unwrap_or(Value::Null),
                    Value::Int(id as i64),
                ],
            )?;
        }
    }
    transaction.commit()?;
    Ok(())
}

fn catch_up(
    db: &mut Database,
    consumer: &mut SearchProjectionConsumer,
    build: &VectorIndexBuildOptions,
    phase: &str,
) -> Result<VectorIndexUpdateReport> {
    let epoch = consumer.canonical_vector_source_epoch()?;
    let generation = consumer
        .projection()
        .stationary_receipt()?
        .generation_name()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempts = 0usize;
    loop {
        let error = match db.catch_up_vector_index(consumer, build.clone()) {
            Ok(report) => return Ok(report),
            Err(error) => error,
        };
        let (code, resource, requested, available) = match &error {
            VectorIndexBuildError::Admission(error)
                if error.code == RuntimeAdmissionCode::BackgroundTaskSaturated
                    && error.is_retryable() =>
            {
                (
                    error.code.as_str(),
                    None,
                    Some(error.requested),
                    Some(error.available),
                )
            }
            VectorIndexBuildError::Consumer(SearchProjectionConsumerError::Database(
                HawDBError::BranchBusy { resource },
            ))
            | VectorIndexBuildError::Query(HawDBError::BranchBusy { resource }) => {
                ("branch_busy", Some(resource), None, None)
            }
            _ => return Err(error.into()),
        };
        if Instant::now() >= deadline {
            return Err(error.into());
        }
        // A stationary receipt refuses pending preparation/selection, including
        // an uncertain publication whose old read domain is still usable.
        if consumer.canonical_vector_source_epoch()? != epoch
            || consumer
                .projection()
                .stationary_receipt()?
                .generation_name()
                != generation
        {
            return Err("retryable refusal changed the Consumer selection".into());
        }
        attempts += 1;
        println!(
            "{}",
            json!({"event": "admission_retry", "phase": phase,
            "attempt": attempts, "code": code, "resource": resource,
            "requested": requested, "available": available})
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

pub(super) fn run(arguments: &[String]) -> Result<()> {
    if arguments.len() != 7 {
        return Err("canonical requires root, count, dimension, memory-MiB, plain|lsg, mutation-per-mille and samples".into());
    }
    let root = Path::new(&arguments[0]);
    let count = arguments[1].parse::<usize>()?;
    let dimension = arguments[2].parse::<usize>()?;
    let memory = arguments[3]
        .parse::<usize>()?
        .checked_mul(1 << 20)
        .ok_or("memory overflow")?;
    let lsg = match arguments[4].as_str() {
        "plain" => false,
        "lsg" => true,
        _ => return Err("invalid graph mode".into()),
    };
    let rate = arguments[5].parse::<usize>()?;
    let samples = arguments[6].parse::<usize>()?;
    if !(1_000..=100_000).contains(&count)
        || !(3..=1024).contains(&dimension)
        || !((8 << 20)..=(512 << 20)).contains(&memory)
        || !(1..=400).contains(&rate)
        || !(1..=128).contains(&samples)
    {
        return Err("canonical fixture bounds exceeded".into());
    }
    let changes = (count * rate / 1000 / 4 * 4).max(4);
    let mut stride = 7919usize;
    loop {
        let (mut left, mut right) = (count, stride);
        while right != 0 {
            (left, right) = (right, left % right);
        }
        if left == 1 {
            break;
        }
        stride += 1;
    }
    let fixture = Fixture {
        count,
        dimension,
        changes: (0..changes).map(|index| index * stride % count).collect(),
    };
    std::fs::create_dir(root)?;
    let mut config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        strict_capacity_limit: true,
        ..Default::default()
    };
    // Declare the atomic mutation envelope; undersized capture still requires
    // an explicit rebuild and is covered independently by library regressions.
    config
        .search_projection_relational_change_limits
        .max_entries = NonZeroUsize::new(
        config
            .search_projection_relational_change_limits
            .max_entries
            .get()
            .max(changes),
    )
    .unwrap();
    let change_capture = config.search_projection_relational_change_limits;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            // Source staging, publication and retained read domains coexist.
            memory_budget_bytes: Some(2 << 30),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let mut db = Database::open_with_config(root, config.clone())?;
    db.set_runtime_governor(governor.clone());
    db.query_sql(&format!("CREATE TABLE vectors (id BIGINT PRIMARY KEY, coarse_cluster BIGINT, fine_cluster BIGINT, embedding VECTOR({dimension}))"))?;
    db.query_sql("CREATE INDEX vectors_fine ON vectors (fine_cluster)")?;
    db.query_sql("CREATE INDEX vectors_coarse ON vectors (coarse_cluster)")?;
    let started = Instant::now();
    for first in (0..count).step_by(1000) {
        let mut transaction = db.begin_transaction()?;
        for id in first..count.min(first + 1000) {
            transaction.query_sql_with_params(
                "INSERT INTO vectors (id, coarse_cluster, fine_cluster, embedding) VALUES ($1, $2, $3, $4)",
                &[Value::Int(id as i64), Value::Int((id % 100 / 10) as i64), Value::Int((id % 100) as i64),
                    Value::Vector(Vector::new(&vector(id as u64, dimension, id % 100))?)],
            )?;
        }
        transaction.commit()?;
        let rows = count.min(first + 1000);
        println!(
            "{}",
            json!({"phase": "ingest", "rows": rows, "elapsed_ns": started.elapsed().as_nanos()})
        );
        // Seal initial batches within the default live-row overlay envelope.
        if rows.is_multiple_of(10_000) || rows == count {
            let checkpoint = Instant::now();
            db.checkpoint()?;
            println!(
                "{}",
                json!({"phase": "ingest_checkpoint", "rows": rows,
                "elapsed_ns": checkpoint.elapsed().as_nanos()})
            );
        }
    }
    resources(&db, &governor, "source_checkpoint", started)?;
    let mut build = VectorIndexBuildOptions {
        memory_bytes: NonZeroUsize::new(memory).unwrap(),
        max_documents: NonZeroUsize::new(count + changes).unwrap(),
        ..Default::default()
    };
    // Fund ordered key seeks, row emission and the final source probe.
    let key_seek_work = 2 + (usize::BITS - count.leading_zeros()) as usize;
    let source_work = (count + 1) * (key_seek_work + 1) + 2;
    build.feed.work = NonZeroUsize::new(source_work.max(build.feed.work.get())).unwrap();
    let VectorIndexBackend::DiskAnn { lsg: settings, .. } = &mut build.backend else {
        return Err("default backend is not DiskANN".into());
    };
    *settings = lsg.then_some(VectorIndexLsg {
        sample_size: 256,
        neighbors: 10,
        exponent_milli: 1000,
    });
    println!(
        "{}",
        json!({"phase": "configuration", "protocol": VERSION, "seed": SEED, "count": count,
        "dimension": dimension, "build_memory_bytes": memory, "feed_work_budget": build.feed.work.get(),
        "cold_reopen_memory_bytes": memory,
        "lsg": lsg, "mutation_per_mille": rate,
        "changed_keys": changes, "samples": samples, "durability": "SyncOnEveryWrite",
        "source_change_capture_entries": change_capture.max_entries.get(),
        "source_change_capture_bytes": change_capture.max_bytes.get()})
    );
    let started = Instant::now();
    let destination = root.join("index");
    let mut consumer = db.create_vector_index(
        "canonical",
        &VectorIndexField::SqlColumn {
            table: "vectors".into(),
            column: "embedding".into(),
        },
        &destination,
        SearchProjectionConsumerOptions::new(NonZeroU64::new(1_000_000).unwrap()),
        build.clone(),
    )?;
    resources(&db, &governor, "build", started)?;
    let original = graphs(root)?;
    if original.is_empty() {
        return Err("canonical build produced no graph artifacts".into());
    }
    let graph_bytes = original
        .keys()
        .map(|path| std::fs::metadata(path).map(|value| value.len()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .sum::<u64>();
    println!(
        "{}",
        json!({"phase": "build", "event": "graph_artifacts", "files": original, "total_bytes": graph_bytes,
        "selection": consumer.projection().stationary_receipt()?.generation_name()})
    );
    let permit = db.search_projection_consumer_read_permit(
        RuntimeWorkRequest::foreground_query(256 << 20, 4 << 20).with_io_wave_slots(1),
    )?;
    let old = db
        .begin_read_transaction()?
        .bind_vector_index(&consumer, &permit)?;
    let baseline = qualify(&old, &permit, &fixture, false, samples, "baseline", None)?;
    for restore in [false, true] {
        let phase = if restore { "restore" } else { "mixed_crud" };
        let started = Instant::now();
        mutate(&mut db, &fixture, restore)?;
        let rows = db.query_sql("SELECT COUNT(*) AS row_count FROM vectors")?;
        if rows.rows[0]["row_count"] != Value::Int(count as i64) {
            return Err("canonical CRUD changed the expected source row count".into());
        }
        let refusal = db
            .begin_read_transaction()?
            .bind_vector_index(&consumer, &permit);
        if !matches!(
            refusal,
            Err(VectorIndexReadError::SourceEpochMismatch { .. })
        ) {
            return Err("stale index bound to a newer source epoch".into());
        }
        let report = catch_up(&mut db, &mut consumer, &build, phase)?;
        let expected_upserts = changes / 4 * if restore { 3 } else { 2 };
        if report.mapped_candidates != changes
            || report.upserted_vectors != expected_upserts
            || report.retraction_candidates != changes - expected_upserts
            || consumer.document_count()? != count - usize::from(!restore) * changes / 4
        {
            return Err(format!("incorrect canonical CRUD mapping: {report:?}").into());
        }
        println!(
            "{}",
            json!({"phase": phase, "event": "publication", "source_epoch": report.source_epoch,
            "base_epoch": report.base_epoch, "mapped_candidates": report.mapped_candidates,
            "upserted_vectors": report.upserted_vectors, "retraction_candidates": report.retraction_candidates,
            "feed_work": report.feed_work, "feed_items": report.feed_items, "feed_bytes": report.feed_bytes})
        );
        resources(&db, &governor, phase, started)?;
        assert_reused(root, &original)?;
        let current = db
            .begin_read_transaction()?
            .bind_vector_index(&consumer, &permit)?;
        qualify(&current, &permit, &fixture, !restore, samples, phase, None)?;
        qualify(
            &old,
            &permit,
            &fixture,
            false,
            samples,
            "old_pin",
            Some(&baseline),
        )?;
        let no_op = catch_up(&mut db, &mut consumer, &build, &format!("{phase}_noop"))?;
        if no_op.mapped_candidates != 0 || no_op.source_epoch != report.source_epoch {
            return Err("no-op catch-up changed its source identity".into());
        }
    }
    drop(old);
    drop(permit);
    drop(consumer);
    db.checkpoint()?;
    drop(db);
    let started = Instant::now();
    let mut db = Database::open_with_config(root, config)?;
    db.set_runtime_governor(governor.clone());
    let consumer: SearchProjectionConsumer = db.open_search_projection_consumer_with_task(
        &SearchProjectionConsumerId::new("canonical")?,
        &destination,
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(
            build.memory_bytes.get() as u64,
            0,
        )),
    )?;
    if consumer
        .vector_index_descriptor()?
        .ok_or("missing reopened descriptor")?
        .backend()
        != build.backend
    {
        return Err("cold reopen changed the graph construction settings".into());
    }
    resources(&db, &governor, "cold_reopen", started)?;
    let permit = db.search_projection_consumer_read_permit(
        RuntimeWorkRequest::foreground_query(256 << 20, 4 << 20).with_io_wave_slots(1),
    )?;
    let read = db
        .begin_read_transaction()?
        .bind_vector_index(&consumer, &permit)?;
    qualify(
        &read,
        &permit,
        &fixture,
        false,
        samples,
        "cold_reopen",
        None,
    )?;
    println!(
        "{}",
        json!({"phase": "canonical_complete", "count": count, "dimension": dimension,
        "lsg": lsg, "changed_keys": changes, "source_epoch": read.source_epoch(), "generation": read.generation()})
    );
    Ok(())
}
