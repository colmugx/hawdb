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

use super::{DurableManifest, WalCursorEvent, WalOpenOutcome, WalRecordCursor};
use crate::config::WalReplayConfig;
use crate::file_io::File;
use hawdb_core::{HawDBError, Result};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Refuse incompatible canonical schemas before any repair or runtime replacement.
pub(super) fn validate_checkpoint_formats(
    manifest: DurableManifest,
    limits: WalReplayConfig,
    resolve: impl Fn(&str) -> Result<PathBuf>,
) -> Result<()> {
    use crate::relational::{
        relational_row_page_manifest_generation_file, RelationalRowPagePublicationConfig,
        RelationalRowPageRootManifest,
    };
    if let Some(generation) = manifest.checkpoint_generation {
        let path = resolve(&crate::artifact_files::checkpoint_generation_file(
            generation,
        ))?;
        let limit = limits.max_checkpoint_encoded_bytes.unwrap_or(u64::MAX);
        let mut encoded = Vec::new();
        File::open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    HawDBError::StorageIntegrity(format!(
                        "missing checkpoint artifact {}",
                        path.display()
                    ))
                } else {
                    error.into()
                }
            })?
            .take(limit.saturating_add(1))
            .read_to_end(&mut encoded)?;
        if encoded.len() as u64 > limit {
            return Err(HawDBError::Storage(
                "checkpoint format admission byte limit exceeded".into(),
            ));
        }
        let digest = hawdb_integrity::integrity_digest(&encoded);
        if Some(encoded.len() as u64) != manifest.checkpoint_encoded_len
            || Some(u64::from(digest.crc32c.get())) != manifest.checkpoint_encoded_checksum
            || Some(digest.sha256) != manifest.checkpoint_encoded_sha256
        {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint format admission identity mismatch".into(),
            ));
        }
        let text = super::read_durable_text_bytes_with_limit(
            &encoded,
            "checkpoint",
            limits.max_checkpoint_decoded_bytes,
        )?;
        let (body, checksum) = crate::checkpoint::split_checkpoint_checksum(&text)?;
        if hawdb_integrity::checksum_u64(body.as_bytes()) != checksum {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint checksum mismatch".into(),
            ));
        }
        if let Some(metadata) = crate::checkpoint::relational_checkpoint_metadata(body)? {
            let path =
                resolve(&crate::artifact_files::relational_checkpoint_generation_file(generation))?;
            super::verify_file_integrity(
                &path,
                metadata.encoded_len,
                metadata.encoded_checksum,
                metadata.encoded_sha256,
                "relational checkpoint",
            )?;
            crate::relational::validate_relational_checkpoint_file_format(&path)
                .map_err(HawDBError::from_storage_error)?;
        }
    }
    if let Some(binding) = manifest.relational_row_generation_artifacts {
        let path = resolve(&relational_row_page_manifest_generation_file(
            binding.generation,
        ))?;
        let decoded = RelationalRowPageRootManifest::read_bound_file(
            &path,
            RelationalRowPagePublicationConfig::default(),
            binding.manifest_artifact,
        )
        .map_err(HawDBError::from_storage_error)?;
        if decoded.generation != binding.generation
            || decoded.source_commit_epoch != binding.source_commit_epoch
            || decoded.root_set_digest != binding.root_set_digest
        {
            return Err(HawDBError::StorageIntegrity(
                "relational schema binding differs".into(),
            ));
        }
    }
    if let Some(binding) = manifest.append_generation_artifacts {
        let path = resolve(&crate::append_table::append_generation_manifest_file(
            binding.generation,
        ))?;
        let artifact = binding.manifest_artifact;
        super::verify_file_integrity(
            &path,
            artifact.encoded_len,
            u64::from(artifact.encoded_crc32c),
            artifact.encoded_sha256,
            "append manifest",
        )?;
        let decoded = crate::append_table::AppendGenerationManifest::read_file(
            &path,
            crate::append_table::AppendPublicationConfig::default(),
        )
        .map_err(HawDBError::from_storage_error)?;
        if decoded.generation != binding.generation
            || decoded.source_commit_epoch != binding.source_commit_epoch
            || decoded.root_set_digest != binding.root_set_digest
        {
            return Err(HawDBError::StorageIntegrity(
                "append schema binding differs".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_sealed_formats(
    objects: &crate::immutable_object::ImmutableObjectStore,
    root: &crate::sealed_root::SealedRoot,
    manifest: DurableManifest,
    remaining: &mut WalReplayConfig,
) -> Result<()> {
    validate_checkpoint_formats(manifest, *remaining, |name| {
        let binding = root
            .checkpoint_bindings
            .iter()
            .find(|binding| binding.relative_path == name)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "sealed root is missing schema artifact {name}"
                ))
            })?;
        Ok(objects.object_path(binding.reference))
    })?;
    let mut epoch = root.checkpoint_epoch;
    for interval in &root.sealed_wals {
        epoch = validate_wal_formats(
            &objects.object_path(interval.object),
            epoch,
            remaining,
            false,
        )?;
    }
    Ok(())
}

pub(super) fn validate_wal_formats(
    path: &Path,
    epoch: u64,
    remaining: &mut WalReplayConfig,
    allow_torn_tail: bool,
) -> Result<u64> {
    validate_wal_formats_from_file(File::open(path)?, epoch, remaining, allow_torn_tail)
        .map(|(epoch, _)| epoch)
}

pub(super) fn validate_wal_formats_from_file(
    file: File,
    mut epoch: u64,
    remaining: &mut WalReplayConfig,
    allow_torn_tail: bool,
) -> Result<(u64, File)> {
    let mut cursor = match WalRecordCursor::from_file(file, remaining.max_record_bytes)? {
        WalOpenOutcome::Cursor(cursor) => cursor,
        _ => {
            return Err(HawDBError::StorageIntegrity(
                "WAL format admission rejected its header".into(),
            ))
        }
    };
    loop {
        match cursor.next()? {
            WalCursorEvent::Entry {
                entry, encoded_len, ..
            } => {
                if let Some(entries) = &mut remaining.max_entries {
                    *entries = entries.checked_sub(1).ok_or_else(|| {
                        HawDBError::Storage(
                            "WAL replay entry limit exceeded during format admission".into(),
                        )
                    })?;
                }
                if let Some(bytes) = &mut remaining.max_bytes {
                    *bytes = bytes.checked_sub(encoded_len).ok_or_else(|| {
                        HawDBError::Storage(
                            "WAL replay byte limit exceeded during format admission".into(),
                        )
                    })?;
                }
                epoch = epoch
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::StorageIntegrity("WAL epoch overflow".into()))?;
                super::immutable_root::validate_source_wal_op(
                    &entry.op,
                    epoch,
                    remaining.max_batch_operations,
                )?;
            }
            WalCursorEvent::Eof => return Ok((epoch, cursor.into_file())),
            WalCursorEvent::TornTail { .. } | WalCursorEvent::Corrupt { .. } if allow_torn_tail => {
                // Ordinary recovery owns torn-tail policy and corruption evidence.
                return Ok((epoch, cursor.into_file()));
            }
            WalCursorEvent::TornTail { reason, .. } | WalCursorEvent::Corrupt { reason, .. } => {
                return Err(HawDBError::StorageIntegrity(reason));
            }
        }
    }
}
