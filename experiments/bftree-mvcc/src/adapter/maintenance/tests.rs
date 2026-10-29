// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use tempfile::tempdir;

fn append_record(path: &Path, generation: u64, payload: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    let offset = file.metadata().unwrap().len() - PREFIX - WAL_BINARY_FILE_HEADER_BYTES as u64;
    file.write_all(&frame_binary_wal_record(generation, payload, offset))
        .unwrap();
    file.sync_all().unwrap();
}

#[test]
fn checksummed_but_invalid_gc_plans_cannot_discard_an_anchor() {
    for entries in [
        vec![(b"key".to_vec(), 1)],
        vec![(b"key".to_vec(), 2)],
        vec![(b"key".to_vec(), 1), (b"key".to_vec(), 1)],
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open(&path).unwrap();
        for epoch in 1..=2 {
            let mut tx = store.begin_write().unwrap();
            tx.put(b"key", &[epoch as u8]).unwrap();
            tx.commit(epoch).unwrap();
        }
        drop(store);
        append_record(
            &path,
            Layout::Hybrid.id(),
            &GcPlan {
                epoch: 2,
                horizon: 1,
                entries,
            }
            .encode(),
        );
        let bytes = std::fs::read(&path).unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn checksummed_checkpoint_must_cover_exact_metadata_and_current_values() {
    for case in 0..4 {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open(&path).unwrap();
        let mut tx = store.begin_write().unwrap();
        tx.put(b"key", b"value").unwrap();
        tx.commit(1).unwrap();
        let end = store.journal_bytes().unwrap();
        drop(store);
        let row = SavedVersion {
            key: b"key".to_vec(),
            epoch: 1,
            value: Some(if case == 1 {
                b"wrong".to_vec()
            } else {
                b"value".to_vec()
            }),
        };
        let mut snapshot = Snapshot {
            epoch: 1,
            covered_end: end,
            rows: vec![row],
        };
        if case == 0 {
            snapshot.covered_end += 1;
        }
        if case == 2 {
            snapshot.rows.clear();
        }
        if case == 3 {
            snapshot.rows[0].epoch = 2;
        }
        append_record(&path, Layout::Hybrid.id(), &snapshot.encode().unwrap());
        let bytes = std::fs::read(&path).unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn unacknowledged_tail_loss_tearing_and_block_reordering_keep_durable_prefix_evidence() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source");
    let store = Store::open(&source).unwrap();
    let mut seed = store.begin_write().unwrap();
    seed.put(b"durable", b"old").unwrap();
    seed.commit(1).unwrap();
    store.checkpoint().unwrap();
    drop(store);
    let base = std::fs::read(&source).unwrap();
    let writes = (0..MAX_WRITES as u64)
        .map(|n| Mutation {
            key: n.to_be_bytes().to_vec(),
            value: Some(vec![7; MAX_VALUE]),
        })
        .collect();
    let batch = Batch {
        epoch: 2,
        operation: 2,
        writes,
    };
    let frame = frame_binary_wal_record(
        Layout::Hybrid.id(),
        &batch.encode(),
        base.len() as u64 - PREFIX - WAL_BINARY_FILE_HEADER_BYTES as u64,
    );
    let mut reordered = frame.clone();
    let block = hawdb_storage::wal::frame::WAL_BLOCK_BYTES;
    let first_full =
        block - ((base.len() - PREFIX as usize - WAL_BINARY_FILE_HEADER_BYTES) % block);
    assert!(first_full + block * 2 < reordered.len());
    let (before, after) = reordered.split_at_mut(first_full + block);
    before[first_full..].swap_with_slice(&mut after[..block]);
    let cases = [
        Vec::new(),
        frame.clone(),
        frame[..1].to_vec(),
        frame[..frame.len() / 2].to_vec(),
        frame[..frame.len() - 1].to_vec(),
        reordered,
        {
            let mut partial = frame.clone();
            let end = partial.len();
            partial[end - 32..].fill(0);
            partial
        },
    ];
    for (case, tail) in cases.into_iter().enumerate() {
        let path = dir.path().join(format!("image-{case}"));
        let bytes = [base.clone(), tail].concat();
        std::fs::write(&path, &bytes).unwrap();
        match Store::open(&path) {
            Ok(recovered) if case <= 1 => {
                let view = recovered.begin_read().unwrap();
                assert_eq!(view.get(b"durable").unwrap(), Some(b"old".to_vec()));
                assert_eq!(view.epoch(), if case == 0 { 1 } else { 2 });
                for change in &batch.writes {
                    assert_eq!(
                        view.get(&change.key).unwrap(),
                        if case == 0 {
                            None
                        } else {
                            change.value.clone()
                        }
                    );
                }
            }
            Err(_) if case >= 2 => {}
            _ => panic!("unexpected power-loss image result {case}"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(&bytes[..base.len()], base.as_slice());
    }
}

#[test]
fn released_snapshot_horizon_finishes_old_pass_then_collects_skipped_prefix_keys() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("journal")).unwrap();
    let mut tx = store.begin_write().unwrap();
    for n in 0..32_u64 {
        tx.put(&n.to_be_bytes(), b"old").unwrap();
    }
    tx.commit(1).unwrap();
    let old = store.begin_read().unwrap();
    let mut tx = store.begin_write().unwrap();
    for n in 0..32_u64 {
        tx.put(&n.to_be_bytes(), b"new").unwrap();
    }
    tx.commit(2).unwrap();
    let first = store.collect_garbage(3).unwrap();
    assert_eq!(first.horizon, 1);
    assert!(!first.complete_pass);
    drop(old);
    for _ in 0..100 {
        let step = store.collect_garbage(3).unwrap();
        assert!(step.examined <= 3);
        if step.complete_pass {
            assert_eq!(step.horizon, 2);
            assert_eq!(store.storage_stats().unwrap().retained_versions, 32);
            return;
        }
    }
    panic!("collector did not catch up after snapshot release");
}
