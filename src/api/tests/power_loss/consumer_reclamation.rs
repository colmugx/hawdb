// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{SearchProjectionConsumerId, SearchProjectionConsumerOptions, SearchRebuildOptions};
use std::io::Write;
use std::num::NonZeroU64;

#[test]
fn registered_consumer_links_sealed_producer_inodes_before_selection_and_scratch_retirement() {
    let fixture = Fixture::new();
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
    database
        .query("CREATE (:Memory {id: 'm1', content: 'linked immutable document'})")
        .unwrap();
    fixture
        .model
        .observe(ObservationPoint {
            event: IoEvent::HardLink,
            relative_path: "projection".into(),
            boundary: ObservationBoundary::After,
            skip_matches: 0,
            include_descendants: true,
            keep_last: true,
        })
        .unwrap();
    let id = SearchProjectionConsumerId::new("linked-producer").unwrap();
    let target = fixture.root.join("projection");
    let consumer = database
        .create_search_projection_consumer(
            id.clone(),
            &target,
            SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap()),
            |snapshot, index| {
                snapshot
                    .rebuild_search_projection(index, SearchRebuildOptions::default())
                    .map(|_| ())
            },
        )
        .unwrap();
    let cut = fixture
        .model
        .take_observation()
        .unwrap()
        .expect("registered installation must link a sealed artifact");
    let linked = cut.observed_path().unwrap();
    let before_barrier = cut.crash(&CrashPlan::default()).unwrap();
    assert!(
        before_barrier.bytes(linked).is_none(),
        "the new link has no target-directory barrier yet"
    );
    let candidate = cut.crash(&cut.persist_all_plan()).unwrap();
    let inode = candidate.file_inode(linked).unwrap();
    let originals = candidate
        .paths()
        .filter(|path| {
            path.components()
                .any(|component| component.as_os_str() == ".consumer-producer")
                && candidate.file_inode(path) == Some(inode)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        originals.len(),
        1,
        "the installed artifact must share its sealed producer inode"
    );
    let bytes = candidate.bytes(originals[0]).unwrap();
    assert_eq!(candidate.bytes(linked).unwrap(), bytes);
    let acknowledged = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(acknowledged.bytes(linked).unwrap(), bytes);
    assert!(!acknowledged.directory_paths().any(|path| {
        path.components()
            .any(|component| component.as_os_str() == ".consumer-producer")
    }));
    assert_eq!(consumer.search_index().unwrap().document_count(), 1);
    drop(consumer);
    drop(database);
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
    let reopened = database
        .open_search_projection_consumer(&id, &target)
        .unwrap();
    assert_eq!(reopened.search_index().unwrap().document_count(), 1);
    drop(reopened);
    drop(database);
    // The registry seals an absolute Consumer locator. Recreate the modeled
    // crash at the same path after releasing the recorder and all live owners.
    let root = fixture.root.clone();
    drop(fixture);
    let image = MaterializedImage(root);
    acknowledged.materialize(&image).unwrap();
    let mut recovered = open(&image, DurabilityPolicy::SyncOnEveryWrite);
    let consumer = recovered
        .open_search_projection_consumer(&id, &target)
        .unwrap();
    assert_eq!(consumer.document_count().unwrap(), 1);
    assert_eq!(
        recovered
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows[0]["id"],
        Value::String("m1".into())
    );
}

#[test]
fn consumer_reclamation_persists_retirement_authority_before_the_first_unlink() {
    let fixture = Fixture::new();
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
    database
        .query("CREATE (:Memory {id: 'm1', content: 'document one'})")
        .unwrap();
    let id = SearchProjectionConsumerId::new("retirement-barrier").unwrap();
    let target = fixture.root.join("projection");
    let consumer = database
        .create_search_projection_consumer(
            id.clone(),
            &target,
            SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap()),
            |snapshot, index| {
                snapshot
                    .rebuild_search_projection(index, SearchRebuildOptions::default())
                    .map(|_| ())
            },
        )
        .unwrap();
    let ledger_path = fixture.root.join("consumer-allocations.hawdb");
    let selected = std::fs::read(&ledger_path).unwrap();
    database.unregister_search_projection_consumer(&id).unwrap();
    drop(consumer);
    let retired = std::fs::read(&ledger_path).unwrap();
    assert_ne!(selected, retired);

    // Model a lost final ledger barrier: the retirement is visible, while the
    // durable image still selects the generation about to be reclaimed.
    let mut file = hawdb_storage::file_io::File::create(&ledger_path).unwrap();
    file.write_all(&selected).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut file = hawdb_storage::file_io::File::create(&ledger_path).unwrap();
    file.write_all(&retired).unwrap();
    drop(file);
    let ledger = Path::new("consumer-allocations.hawdb");
    let before = fixture.model.capture().unwrap();
    assert!(!before.uncovered_writes(ledger).unwrap().is_empty());
    assert_eq!(
        before.crash(&CrashPlan::default()).unwrap().bytes(ledger),
        Some(selected.as_slice())
    );
    fixture
        .model
        .observe(ObservationPoint {
            event: IoEvent::Remove,
            relative_path: "projection".into(),
            boundary: ObservationBoundary::Before,
            skip_matches: 0,
            include_descendants: true,
            keep_last: false,
        })
        .unwrap();
    let report = database
        .retry_search_projection_consumer_cleanup(&id, &target, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert_eq!(report.reclaimed_generations, 1, "{report:?}");
    let unlink = fixture.model.take_observation().unwrap().unwrap();
    assert!(unlink.uncovered_writes(ledger).unwrap().is_empty());
    for plan in publication_fault_plans(&unlink) {
        assert_eq!(
            unlink.crash(&plan).unwrap().bytes(ledger),
            Some(retired.as_slice()),
            "retirement authority must survive every uncovered-write schedule: {plan:?}"
        );
    }
    let completed = fixture.model.capture().unwrap();
    let durable = completed.crash(&CrashPlan::default()).unwrap();
    assert!(!durable
        .directory_paths()
        .any(|path| path == Path::new("projection")));
    assert_eq!(durable.bytes(ledger), Some(retired.as_slice()));
}
