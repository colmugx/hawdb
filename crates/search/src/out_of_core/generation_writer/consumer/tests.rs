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
use hawdb_core::{RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
};
use std::sync::{mpsc, Arc};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Acquiring,
    Finished,
}

#[derive(Debug)]
struct ObservedIo {
    task: RuntimeTaskContext,
    events: mpsc::Sender<Event>,
}

#[test]
fn canonical_vector_builder_uses_the_admitted_memory_ceiling() {
    use hawdb_vector_projection::diskann::{DiskAnnBuildConfig, DiskAnnIndex};
    let root = super::super::tests::test_dir("vector-consumer-memory");
    let index = vector_index_with_backend(&root, hawdb_core::VectorIndexBackend::default());
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(128 << 20))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    let options =
        consumer_options(&index, NonZeroU64::new(256 << 20).unwrap(), &memory, &task).unwrap();
    let required = DiskAnnIndex::build_memory_bound(
        100_000,
        128,
        DiskAnnBuildConfig {
            max_working_bytes: options.rabitq_build_memory_bytes.get(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(required > 64 << 20);
    assert!(required < 128 << 20);
    assert_eq!(options.rabitq_build_memory_bytes.get(), 128 << 20);
    let _construction = memory.retained.reserve(required).unwrap();
}
impl RuntimeIoWaveController for ObservedIo {
    fn try_acquire(
        &self,
        _: NonZeroUsize,
        _: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        panic!("producer uses a cancellable synchronous wave");
    }
    fn acquire(
        &self,
        slots: NonZeroUsize,
        _: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        self.events.send(Event::Acquiring).unwrap();
        Ok(self.task.acquire_io_wave(slots)?.expect("governed wave"))
    }
}

#[test]
fn vector_index_producer_waits_for_original_io_wave_before_writing_and_can_cancel() {
    let root = super::super::tests::test_dir("vector-consumer-io");
    std::fs::create_dir_all(&root).unwrap();
    let publication = super::super::super::SearchProjectionPublishLease::acquire(&root).unwrap();
    let governor = governor();
    let blocker = governor
        .try_admit(
            RuntimeWorkRequest::io(RuntimeWorkPriority::Background, 0, 0).with_io_wave_slots(1),
        )
        .unwrap();
    let blocker_task = blocker.bind_task_context(Default::default());
    let held_wave = blocker_task.acquire_io_wave(NonZeroUsize::MIN).unwrap();
    let permit = Arc::new(
        governor
            .try_admit(RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1))
            .unwrap(),
    );
    let task = permit.bind_task_context(Default::default());
    let cancellation = task.cancellation().clone();
    let (events, receiver) = mpsc::channel();
    let task = task.clone().with_io_wave_controller(Arc::new(ObservedIo {
        task,
        events: events.clone(),
    }));
    let children = || {
        std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let original_children = children();
    let index = vector_index(&root);
    let worker_permit = Arc::clone(&permit);
    let worker = std::thread::spawn(move || {
        let result = SearchOutOfCoreGenerationWriter::write_consumer(
            &index,
            &publication,
            task,
            NonZeroU64::new(256 << 20).unwrap(),
            worker_permit,
        );
        events.send(Event::Finished).unwrap();
        (index, publication, result)
    });
    let event = receiver.recv_timeout(std::time::Duration::from_secs(30));
    // Cancellation and join happen before asserting, including the unfixed path.
    let untouched = children() == original_children;
    cancellation.cancel();
    let (index, publication, result) = worker.join().unwrap();
    assert_eq!(event.unwrap(), Event::Acquiring);
    assert!(
        untouched,
        "producer wrote while the original I/O slot was occupied"
    );
    assert!(result.is_err());
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    drop(held_wave);
    drop(blocker_task);
    drop(blocker);
    SearchOutOfCoreGenerationWriter::write_consumer(
        &index,
        &publication,
        permit.bind_task_context(Default::default()),
        NonZeroU64::new(256 << 20).unwrap(),
        Arc::clone(&permit),
    )
    .unwrap();
    assert!(root.join(crate::SEARCH_SNAPSHOT_FILE).exists());
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(publication);
    drop(index);
    drop(permit);
    std::fs::remove_dir_all(root).unwrap();
}

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(256 << 20),
            background_task_limit: Some(NonZeroUsize::new(2).unwrap()),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(1, 1),
    )
}

fn vector_index(root: &std::path::Path) -> SearchIndex {
    vector_index_with_backend(
        root,
        hawdb_core::VectorIndexBackend::RaBitQ {
            bit_width: 4,
            segment_rows: 64,
            transform_seed: 42,
        },
    )
}

fn vector_index_with_backend(
    root: &std::path::Path,
    backend: hawdb_core::VectorIndexBackend,
) -> SearchIndex {
    let mut index = SearchIndex::in_memory();
    index
        .bind_vector_index(
            hawdb_core::VectorIndexDescriptor::new(
                "docs".into(),
                hawdb_core::VectorIndexTarget::Relational {
                    table: "docs".into(),
                    column: "embedding".into(),
                    column_ordinal: 1,
                },
                hawdb_core::VectorDimension::new(2).unwrap(),
                hawdb_core::VectorDistance::Cosine,
                backend,
                [7; 32],
            )
            .unwrap(),
        )
        .unwrap();
    index.path = Some(root.to_path_buf());
    index.source_graph_commit_epoch = Some(1);
    index.consumer_binding = Some(crate::consumer::ConsumerBinding {
        database_uuid: hawdb_core::generate_uuidv7().unwrap(),
        projection_uuid: hawdb_core::generate_uuidv7().unwrap(),
        consumer_id: "vector-io".into(),
        registration_uuid: hawdb_core::generate_uuidv7().unwrap(),
        checkpoint_uuid: hawdb_core::generate_uuidv7().unwrap(),
    });
    index
}

#[test]
fn successful_consumer_releases_its_stage_under_one_slot_wave_and_task_reservations() {
    for task_scope in [false, true] {
        let root = super::super::tests::test_dir("vector-consumer-cleanup-io");
        std::fs::create_dir_all(&root).unwrap();
        let publication =
            super::super::super::SearchProjectionPublishLease::acquire(&root).unwrap();
        let governor = governor();
        let request = RuntimeWorkRequest::background_maintenance(64 << 20);
        let request = if task_scope {
            request.with_io_slots(1)
        } else {
            request.with_io_wave_slots(1)
        };
        let permit = Arc::new(governor.try_admit(request).unwrap());
        let index = vector_index(&root);
        SearchOutOfCoreGenerationWriter::write_consumer(
            &index,
            &publication,
            permit.bind_task_context(Default::default()),
            NonZeroU64::new(256 << 20).unwrap(),
            Arc::clone(&permit),
        )
        .unwrap();
        let cleanup = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 0).unwrap();
        assert_eq!(cleanup.pending_stages, 0, "{cleanup:?}");
        assert_eq!(cleanup.retained_memory_bytes, 0);
        assert_eq!(cleanup.reserved_disk_bytes, 0);
        drop(publication);
        drop(index);
        drop(permit);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
