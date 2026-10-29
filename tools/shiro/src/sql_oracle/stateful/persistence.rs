// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb::{DatabaseConfig, RelationalIndexMode, StorageResidencyMode};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StorageProfile {
    Memory,
    Shadow,
    DemandPaged,
}

impl StorageProfile {
    pub(super) fn for_case(index: usize) -> Self {
        match index % 3 {
            0 => Self::Memory,
            1 => Self::Shadow,
            _ => Self::DemandPaged,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Shadow => "disk_shadow",
            Self::DemandPaged => "disk_demand_paged",
        }
    }

    fn config(self) -> DatabaseConfig {
        DatabaseConfig {
            relational_index_mode: match self {
                Self::Memory | Self::Shadow => RelationalIndexMode::Shadow,
                Self::DemandPaged => RelationalIndexMode::DemandPaged,
            },
            // Views require resident source rows; their indexes may page from
            // disk. Keep checkpoint timing under the deterministic schedule.
            storage_residency_mode: StorageResidencyMode::Materialized,
            automatic_checkpoint_max_age: Duration::from_secs(3600),
            ..DatabaseConfig::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Boundary {
    None,
    Wal,
    Checkpoint,
}

impl Boundary {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Wal => "wal_reopen",
            Self::Checkpoint => "checkpoint_reopen",
        }
    }
}

pub(super) struct Fixture {
    path: Option<PathBuf>,
    profile: StorageProfile,
    checkpoint_epoch: Option<u64>,
    preserve: bool,
}

impl Fixture {
    pub(super) fn new(profile: StorageProfile) -> Result<Self, Failure> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let path = if profile == StorageProfile::Memory {
            None
        } else {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shiro-maintenance-{}-{nonce}-{}",
                std::process::id(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ));
            // Claim our own directory; never reuse or remove an existing path.
            std::fs::create_dir(&path).map_err(|err| Failure {
                signature: "fixture_create".into(),
                evidence: json!({"error": err.to_string()}),
            })?;
            Some(path)
        };
        Ok(Self {
            path,
            profile,
            checkpoint_epoch: None,
            preserve: false,
        })
    }

    pub(super) fn open(&self) -> Result<Database, Failure> {
        match &self.path {
            Some(path) => Database::open_with_config(path, self.profile.config())
                .map_err(|err| error("open", err)),
            None => Ok(Database::new()),
        }
    }

    pub(super) fn preserve_failure(&mut self, failure: &mut Failure) {
        if let Some(path) = &self.path {
            self.preserve = true;
            failure.evidence["database_path"] = json!(path);
        }
    }

    pub(super) fn checkpoint(&mut self, db: &mut Database) -> Result<(), Failure> {
        if self.path.is_some() {
            let epoch = db
                .commit_epoch()
                .map_err(|err| error("checkpoint_epoch", err))?;
            db.checkpoint().map_err(|err| error("checkpoint", err))?;
            self.checkpoint_epoch = Some(epoch);
        }
        Ok(())
    }

    pub(super) fn reopen(
        &mut self,
        mut db: Database,
        case: &Case,
        boundary: Boundary,
        progress: &mut Progress,
    ) -> Result<Database, Failure> {
        if self.path.is_none() || boundary == Boundary::None {
            return Ok(db);
        }
        let epoch = db
            .commit_epoch()
            .map_err(|err| error("before_close_epoch", err))?;
        // Hold the acknowledged result outside the database. Comparing two
        // recovered tables alone could accept loss of both tables' WAL tail.
        let expected = observe(&case.relations, "before_close", |sql, params| {
            db.query_sql_with_params(sql, params)
        })?;
        if boundary == Boundary::Checkpoint && self.checkpoint_epoch != Some(epoch) {
            self.checkpoint(&mut db)?;
        }
        drop(db);
        let mut reopened = self.open()?;
        let recovered = observe(&case.relations, "recovered", |sql, params| {
            reopened.query_sql_with_params(sql, params)
        })?;
        unchanged(&case.relations, "recovery_changed", &expected, &recovered)?;
        let report = reopened
            .storage_recovery_report()
            .map_err(|err| error("recovery_report", err))?;
        let recovered_epoch = reopened
            .commit_epoch()
            .map_err(|err| error("recovered_epoch", err))?;
        if !report.durable
            || recovered_epoch != epoch
            || report.recovered_commit_epoch != epoch
            || self
                .checkpoint_epoch
                .is_some_and(|expected| report.checkpoint_commit_epoch != Some(expected))
            || (self.checkpoint_epoch.is_none() && report.replayed_wal_entries == 0)
            || (boundary == Boundary::Checkpoint && report.replayed_wal_entries != 0)
        {
            return Err(Failure {
                signature: "recovery_boundary".into(),
                evidence: json!({"boundary": boundary.as_str(), "expected_epoch": epoch,
                    "expected_checkpoint_epoch": self.checkpoint_epoch,
                    "recovered_epoch": recovered_epoch, "report": format!("{report:?}")}),
            });
        }
        if boundary == Boundary::Checkpoint {
            progress.checkpoint_reopens += 1;
        } else {
            progress.wal_reopens += 1;
        }
        if report.replayed_wal_entries > 0 {
            if self.checkpoint_epoch.is_some() {
                progress.checkpoint_wal_replays += 1;
            } else {
                progress.initial_wal_replays += 1;
            }
        }
        Ok(reopened)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.preserve
            && let Some(path) = &self.path
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}
