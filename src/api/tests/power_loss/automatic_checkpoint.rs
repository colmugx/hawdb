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

//! Physical images of the automatic publisher across successive generations.
//! These schedules use the shared POSIX synchronization/rename assumptions.

use super::*;
use std::time::{Duration, Instant};

fn wait_unattended_completion(database: &Database, previous: u64) {
    let control = database.runtime.checkpoint_control_for_test();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let report = database.automatic_checkpoint_report().unwrap().unwrap();
        if report.completed_checkpoints > previous && control.is_idle_for_test().unwrap() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "automatic publication/adoption/retirement did not finish: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn assert_generations(database: &mut Database, generations: usize) {
    let rows = database
        .query("MATCH (n:AutomaticTail) RETURN n.id AS id ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), generations);
    let sources = database
        .query("MATCH (n:Source) RETURN n.id AS id, n.body AS body ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(sources.len(), generations);
    for generation in 1..=generations {
        assert_eq!(sources[generation - 1]["id"], Value::Int(generation as i64));
        assert_eq!(
            sources[generation - 1]["body"],
            Value::String(source_body(generation))
        );
    }
    for generation in 1..=3 {
        let columns = database.query_sql(&format!(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'auto_{generation}'"
        )).unwrap();
        assert_eq!(columns.rows.len(), 1);
        let row = database
            .query_sql(&format!("SELECT id FROM auto_{generation}"))
            .unwrap();
        if generation <= generations {
            assert_eq!(rows[generation - 1]["id"], Value::Int(generation as i64));
            assert_eq!(row.rows.len(), 1);
            assert_eq!(row.rows[0]["id"], Value::Int(generation as i64));
        } else {
            assert!(
                row.rows.is_empty(),
                "row survived without its complete transaction"
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum AutomaticCut {
    Head,
    CandidateWalSync,
    SealedRoot,
    Manifest,
}

impl AutomaticCut {
    fn observation(
        self,
        root: &Path,
        main: Uuid,
        boundary: ObservationBoundary,
    ) -> ObservationPoint {
        let directory = PathBuf::from("branches").join(main.to_string());
        let (event, relative_path, include_descendants) = match self {
            Self::Head => (IoEvent::Rename, directory.join("branch.head"), false),
            Self::CandidateWalSync => {
                let head = hawdb_storage::branch_head::read_branch_head(
                    &root.join(directory.join("branch.head")),
                )
                .unwrap();
                (
                    IoEvent::FileSync,
                    directory.join(wal_generation_file(head.active_wal.generation + 1)),
                    false,
                )
            }
            Self::SealedRoot => (
                IoEvent::HardLink,
                PathBuf::from("branches/objects/objects/sealed-root"),
                true,
            ),
            Self::Manifest => (
                IoEvent::Rename,
                directory.join("runtime/manifest.hawdb"),
                false,
            ),
        };
        ObservationPoint {
            event,
            relative_path,
            boundary,
            skip_matches: 0,
            include_descendants,
            keep_last: false,
        }
    }
}

fn source_body(generation: usize) -> String {
    format!("source {generation}:界🙂\0").repeat(1024)
}

fn automatic_fault_plans(snapshot: &PowerLossSnapshot) -> Vec<CrashPlan> {
    // Retain every original lost/reordered/isolated persistence schedule.
    let mut plans = publication_fault_plans(snapshot);
    let Some(path) = snapshot.observed_path() else {
        return plans;
    };
    if !path
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with("wal."))
    {
        return plans;
    }
    let complete = snapshot.persist_all_plan();
    for write in snapshot.uncovered_writes(path).unwrap() {
        if write.length < 2 {
            continue;
        }
        for bytes in [0..1, 0..write.length / 2, write.length / 2..write.length] {
            let persistence = complete
                .persistence
                .iter()
                .cloned()
                .map(|operation| {
                    if operation == PersistOperation::Whole(write.operation) {
                        PersistOperation::TornWrite {
                            operation: write.operation,
                            bytes: bytes.clone(),
                        }
                    } else {
                        operation
                    }
                })
                .collect();
            let plan = CrashPlan { persistence };
            if !plans.contains(&plan) {
                plans.push(plan);
            }
        }
    }
    plans
}

fn qualify_automatic_policies(cut: AutomaticCut, durabilities: &[DurabilityPolicy]) {
    let mut images = 0;
    for &durability in durabilities {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let mut fixture = Fixture::new();
            let mut database = Database::open_with_durability_and_config(
                &fixture.root,
                durability,
                DatabaseConfig {
                    max_open_files: 32,
                    strict_capacity_limit: true,
                    automatic_checkpoint_max_age: Duration::from_millis(20),
                    ..DatabaseConfig::default()
                },
            )
            .unwrap();
            commit_mixed(&mut database);
            // DDL requires a synchronous canonical-row schema checkpoint.
            // Install it in the covered baseline so each following graph/SQL
            // transaction leaves debt for the automatic owner to publish.
            for generation in 1..=3 {
                database
                    .query_sql(&format!(
                        "CREATE TABLE auto_{generation} (id BIGINT PRIMARY KEY)"
                    ))
                    .unwrap();
            }
            database.checkpoint().unwrap();
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            for generation in 1..=3 {
                // Arm the physical observation after the transaction response
                // but before any automatic preparation can consume its debt.
                // This suspension only coordinates the fixture; no manual
                // checkpoint drives these future generations.
                let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
                let covered = database.commit_epoch().unwrap();
                let previous = database
                    .automatic_checkpoint_report()
                    .unwrap()
                    .unwrap()
                    .completed_checkpoints;
                let mut transaction = database.begin_transaction().unwrap();
                transaction
                    .query_sql(&format!(
                        "INSERT INTO auto_{generation} (id) VALUES ({generation})"
                    ))
                    .unwrap();
                transaction
                    .query(&format!("CREATE (:AutomaticTail {{id: {generation}}})"))
                    .unwrap();
                transaction
                    .query_with_params(
                        "CREATE (:Source {id: $id, body: $body})",
                        &BTreeMap::from([
                            ("id".into(), Value::Int(generation as i64)),
                            ("body".into(), Value::String(source_body(generation))),
                        ]),
                    )
                    .unwrap();
                transaction.commit().unwrap();
                let epoch = database.commit_epoch().unwrap();
                fixture
                    .model
                    .observe(cut.observation(&fixture.root, main, boundary))
                    .unwrap();
                drop(suspension);
                // Only status observations occur while the owner prepares and
                // publishes. No manual maintenance drives these generations.
                wait_unattended_completion(&database, previous);
                fixture.model.capture().unwrap();
                let snapshot = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .unwrap_or_else(|| {
                        panic!("no actual {cut:?} cut at {boundary:?} generation={generation}")
                    });
                eprintln!("automatic-power-v4 cut={cut:?} policy={durability:?} boundary={boundary:?} generation={generation} path={:?}", snapshot.observed_path());
                for plan in automatic_fault_plans(&snapshot) {
                    let root = fixture.image(&snapshot, &plan);
                    let mut recovered = open(&root, durability);
                    let recovered_epoch = recovered.commit_epoch().unwrap();
                    if durability == DurabilityPolicy::SyncOnEveryWrite
                        || plan == snapshot.persist_all_plan()
                    {
                        assert_eq!(
                            recovered_epoch, epoch,
                            "{durability:?} {boundary:?} generation={generation} {plan:?}"
                        );
                    } else {
                        assert!(recovered_epoch == covered || recovered_epoch == epoch,
                            "partial transaction at {durability:?} {boundary:?} generation={generation}: {recovered_epoch}");
                    }
                    assert!(assert_atomic(&mut recovered, main));
                    assert_generations(
                        &mut recovered,
                        generation - usize::from(recovered_epoch == covered),
                    );
                    images += 1;
                }
                // The owner already adopted and retired without frontend access.
                // Verify every acknowledged value before the next observation.
                assert_eq!(
                    database
                        .query("MATCH (n:AutomaticTail) RETURN n.id AS id")
                        .unwrap()
                        .rows
                        .len(),
                    generation
                );
                let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
                drop(suspension);
            }
        }
    }
    println!("automatic {cut:?} qualification: {images} physical crash images, 3 generations, {} durability modes, both boundaries", durabilities.len());
}

#[test]
fn automatic_head_cuts_preserve_synchronous_transactions_across_three_generations() {
    qualify_automatic_policies(AutomaticCut::Head, &[DurabilityPolicy::SyncOnEveryWrite]);
}

#[test]
fn automatic_head_cuts_preserve_relaxed_transactions_across_three_generations() {
    qualify_automatic_policies(AutomaticCut::Head, &[DurabilityPolicy::SyncOnCheckpoint]);
}

#[test]
fn automatic_wal_sync_cuts_preserve_synchronous_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::CandidateWalSync,
        &[DurabilityPolicy::SyncOnEveryWrite],
    );
}

#[test]
fn automatic_wal_sync_cuts_preserve_relaxed_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::CandidateWalSync,
        &[DurabilityPolicy::SyncOnCheckpoint],
    );
}

#[test]
fn automatic_root_cuts_preserve_synchronous_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::SealedRoot,
        &[DurabilityPolicy::SyncOnEveryWrite],
    );
}

#[test]
fn automatic_root_cuts_preserve_relaxed_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::SealedRoot,
        &[DurabilityPolicy::SyncOnCheckpoint],
    );
}

#[test]
fn automatic_manifest_cuts_preserve_synchronous_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::Manifest,
        &[DurabilityPolicy::SyncOnEveryWrite],
    );
}

#[test]
fn automatic_manifest_cuts_preserve_relaxed_transactions_across_three_generations() {
    qualify_automatic_policies(
        AutomaticCut::Manifest,
        &[DurabilityPolicy::SyncOnCheckpoint],
    );
}
