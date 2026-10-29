// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Failed preparation keeps original native custody, not a pathname to adopt.

use super::*;

#[doc(hidden)]
#[derive(Debug)]
pub struct RegisteredConsumerError {
    pub(super) error: HawDBError,
    pub(super) recovery: Option<RegisteredConsumerRecovery>,
}

impl RegisteredConsumerError {
    pub fn with_recovery(error: HawDBError, recovery: RegisteredConsumerRecovery) -> Self {
        Self {
            error,
            recovery: Some(recovery),
        }
    }

    pub fn into_parts(self) -> (HawDBError, Option<RegisteredConsumerRecovery>) {
        (self.error, self.recovery)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum RegisteredConsumerRecovery {
    Unsealed(Box<UnsealedRegisteredConsumer>),
    Prepared(Box<PreparedRegisteredConsumer>),
}

impl RegisteredConsumerRecovery {
    /// Retain original custody when a reserved build is dropped before its
    /// producer starts. Retry settles the proven no-effect native reservation.
    pub fn unstarted(
        birth: PendingConsumerGeneration,
        locator: PathBuf,
        memory: RuntimeRetainedMemory,
    ) -> Self {
        Self::Unsealed(Box::new(UnsealedRegisteredConsumer {
            birth: Some(birth),
            scratch: None,
            locator,
            target_children: Vec::new(),
            cleaned: false,
            _memory: Some(memory),
        }))
    }

    pub fn locator(&self) -> &Path {
        match self {
            Self::Unsealed(owner) => &owner.locator,
            Self::Prepared(owner) => &owner.locator,
        }
    }

    /// Retry cleanup or finish the same immutable preparation with fresh
    /// admission. No callback is retained or rerun for an already built image.
    pub fn retry(
        mut self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
    ) -> std::result::Result<Option<PreparedRegisteredConsumer>, Box<RegisteredConsumerError>> {
        let result = match &mut self {
            Self::Unsealed(owner) => owner.retire(),
            Self::Prepared(owner) => owner.finish_preparation(permit, task),
        };
        if let Err(error) = result {
            return Err(Box::new(RegisteredConsumerError {
                error,
                recovery: Some(self),
            }));
        }
        Ok(match self {
            Self::Unsealed(_) => None,
            Self::Prepared(owner) => Some(*owner),
        })
    }
}

#[derive(Debug)]
pub(super) struct ProducerScratch {
    pub(super) candidate: CapturedDirectoryCandidate,
    pub(super) directory: Option<SourceDirectory>,
}

impl ProducerScratch {
    pub(super) fn remove(&mut self, parent: &SourceDirectory) -> Result<()> {
        let context = parent.operation_context();
        if self.candidate.removal_completed() {
            parent.sync(&context)?;
            return Ok(());
        }
        if !self.candidate.may_own_entry() {
            return Ok(());
        }
        let directory = match self.directory.take() {
            Some(directory) => directory,
            None => parent.open_created_directory(&context, &self.candidate)?,
        };
        remove_producer(parent, directory, &mut self.candidate)?;
        // Do not forget the original scratch owner while its removed entry can
        // still reappear after power loss. If the barrier fails, the completed
        // removal state above retries this same parent without reopening a name.
        parent.sync(&context)?;
        Ok(())
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct UnsealedRegisteredConsumer {
    pub(super) birth: Option<PendingConsumerGeneration>,
    pub(super) scratch: Option<ProducerScratch>,
    pub(super) locator: PathBuf,
    pub(super) target_children: Vec<String>,
    pub(super) cleaned: bool,
    pub(super) _memory: Option<RuntimeRetainedMemory>,
}

impl UnsealedRegisteredConsumer {
    pub(super) fn retire(&mut self) -> Result<()> {
        let birth = self.birth.as_mut().expect("unsealed native owner");
        if !self.cleaned {
            if birth.resolve_unexposed_birth()? {
                return Ok(());
            }
            let directory = birth.unsealed_cleanup_directory()?;
            if let Some(scratch) = self.scratch.as_mut() {
                scratch.remove(directory)?;
            }
            self.scratch = None;
            directory.retire_cached_children();
            let context = directory.operation_context();
            let mut names = Vec::new();
            directory.visit_children(&context, 4096, |name| {
                if name != "consumer-birth.hawdb" {
                    if !self.target_children.iter().any(|known| known == name) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unsealed Consumer retains an unknown child",
                        ));
                    }
                    names.push(name.to_owned());
                }
                Ok(())
            })?;
            for name in names {
                directory.remove_child(&context, &name)?;
            }
            directory.sync(&context)?;
            self.cleaned = true;
        }
        birth.retire_cleaned_unsealed()
    }
}
