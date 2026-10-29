// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Read-only benchmark evidence normalization, including the older experiment
/// identity. This is not an opener, migration, or format-compatibility promise.
/// Maintenance is checked by ordinary recovery; this stream compares exact
/// transaction epochs, operation IDs and mutation payloads across benchmark
/// journals whose framing or maintenance records legitimately differ.
pub fn transaction_stream(path: &Path, layout: Layout, mut output: impl Write) -> Result<()> {
    let mut file = File::open(path)?;
    file.try_lock_shared().map_err(|_| Error::Busy)?;
    let len = file.metadata()?.len();
    if len > MAX_JOURNAL {
        return Err(Error::Budget("journal bytes"));
    }
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    let legacy = &magic == b"BFMVCC01";
    if !legacy && &magic != MAGIC {
        return Err(Error::Invalid("not an experiment audit input"));
    }
    let mut header = [0; WAL_BINARY_FILE_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let (generation, start) =
        decode_binary_wal_header(&header).map_err(|e| Error::Journal(e.to_string()))?;
    if generation != layout.id() || start != 1 {
        return Err(Error::Invalid("audit layout/header mismatch"));
    }
    let mut reader = BinaryWalReader::new(WalFile(file), generation, Some(16 * 1024 * 1024));
    let mut epoch = 0;
    let mut operations = std::collections::BTreeSet::new();
    let mut end = PREFIX + WAL_BINARY_FILE_HEADER_BYTES as u64;
    loop {
        match reader
            .next_event()
            .map_err(|e| Error::Journal(e.to_string()))?
        {
            BinaryWalReadEvent::Record {
                payload,
                end_offset,
                ..
            } => {
                end = PREFIX + end_offset;
                let payload = if legacy {
                    [vec![0], payload].concat()
                } else {
                    payload
                };
                match payload.first() {
                    Some(0) => {
                        if payload.len() > MAX_BATCH {
                            return Err(Error::Budget("audit transaction"));
                        }
                        let batch = Batch::decode(&payload)?;
                        if batch.epoch != epoch + 1 || !operations.insert(batch.operation) {
                            return Err(Error::Invalid("audit transaction sequence"));
                        }
                        epoch = batch.epoch;
                        let canonical = batch.encode();
                        output.write_all(&(canonical.len() as u32).to_le_bytes())?;
                        output.write_all(&canonical)?;
                    }
                    Some(1) | Some(2) if !legacy => {}
                    _ => return Err(Error::Invalid("unknown audit record")),
                }
            }
            BinaryWalReadEvent::Eof if end == len => return Ok(()),
            _ => return Err(Error::Invalid("incomplete audit journal")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn audit_compares_exact_transactions_across_maintenance_and_old_framing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("current");
        let store = Store::open(&path).unwrap();
        let mut tx = store.begin_write().unwrap();
        tx.put(b"key", b"value").unwrap();
        tx.commit(99).unwrap();
        assert!(matches!(
            transaction_stream(&path, Layout::Hybrid, Vec::new()),
            Err(Error::Busy)
        ));
        drop(store);
        let mut before = Vec::new();
        transaction_stream(&path, Layout::Hybrid, &mut before).unwrap();
        let store = Store::open(&path).unwrap();
        store.checkpoint().unwrap();
        drop(store);
        let mut after = Vec::new();
        transaction_stream(&path, Layout::Hybrid, &mut after).unwrap();
        assert_eq!(before, after);
        let payload = &before[5..]; // u32 length and current record discriminator
        let mut legacy = b"BFMVCC01".to_vec();
        legacy.extend(encode_binary_wal_header(Layout::Hybrid.id(), 1));
        legacy.extend(frame_binary_wal_record(Layout::Hybrid.id(), payload, 0));
        let old = dir.path().join("legacy-audit-only");
        std::fs::write(&old, &legacy).unwrap();
        let mut normalized = Vec::new();
        transaction_stream(&old, Layout::Hybrid, &mut normalized).unwrap();
        assert_eq!(before, normalized);
        assert!(Store::open(&old).is_err());
        legacy.pop();
        std::fs::write(&old, legacy).unwrap();
        assert!(transaction_stream(&old, Layout::Hybrid, Vec::new()).is_err());
    }
}
