// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Owned construction and short fenced publication of registered generations.

use super::*;

struct PublicationContext {
    dependencies: PreparedReadDependencies,
    base: Base,
    operations: usize,
    durability: ProjectionSourceDurability,
    expires_at_commit_epoch: u64,
}

struct Construction {
    documents: hawdb_search::SearchAdmittedConsumerDocuments,
    birth: hawdb_storage::consumer_allocation::PendingConsumerGeneration,
    locator: PathBuf,
    limits: RegisteredConsumerLimits,
    recovery_memory: hawdb_qos::RuntimeRetainedMemory,
    permit: Arc<hawdb_qos::RuntimePermit>,
}

/// A reserved successor that borrows only its Consumer, never the database.
/// Release the database owner's write guard before calling `prepare`.
#[must_use]
pub struct ProjectionConsumerPreparation<'consumer> {
    consumer: Option<&'consumer mut SearchProjectionConsumer>,
    context: Option<PublicationContext>,
    construction: Option<Construction>,
}
impl<'consumer> ProjectionConsumerPreparation<'consumer> {
    /// Perform full artifact construction, verification and resident loading.
    /// Even failed preparation returns custody: submit the result to publication
    /// to receive its typed error, or drop it to retain recovery on the Consumer.
    pub fn prepare(mut self) -> ProjectionPreparedConsumerUpdate<'consumer> {
        let consumer = self.consumer.take().expect("reserved Consumer");
        let context = self.context.take().expect("reserved source");
        let prepared = self.construction.take().map(|work| {
            // Acquire on the executing thread after releasing the source owner.
            // Idle tokens retain identity, never an active descriptor inventory.
            let _descriptor_operation = match context.base.descriptor_operation() {
                Ok(operation) => operation,
                Err(error) => {
                    return Err(Box::new(RegisteredConsumerError::with_recovery(
                        error,
                        RegisteredConsumerRecovery::unstarted(
                            work.birth,
                            work.locator,
                            work.recovery_memory,
                        ),
                    )));
                }
            };
            work.documents.prepare_registered(
                work.birth,
                RegisteredConsumerPreparation {
                    locator: &work.locator,
                    epoch: context.dependencies.source_epoch(),
                    permit: work.permit,
                    task: context.base.task.clone(),
                    limits: work.limits,
                    recovery_memory: work.recovery_memory,
                    previous: Some(consumer.projection()),
                    projector: hawdb_search::consumer::RegisteredConsumerProjector::HostMapping,
                },
            )
        });
        ProjectionPreparedConsumerUpdate {
            consumer,
            context: Some(context),
            prepared,
        }
    }
}
impl Drop for ProjectionConsumerPreparation<'_> {
    fn drop(&mut self) {
        if let Some(work) = self.construction.take() {
            let recovery = RegisteredConsumerRecovery::unstarted(
                work.birth,
                work.locator,
                work.recovery_memory,
            );
            let retained = self
                .consumer
                .as_mut()
                .expect("reserved Consumer")
                .projection_mut()
                .retain_failed_preparation(recovery);
            if let Err(error) = retained {
                report_retention_failure(&error);
            }
        }
    }
}

/// Finished private work awaiting source validation and native selection.
/// Dropping it retains original recovery custody on the borrowed Consumer.
#[must_use]
pub struct ProjectionPreparedConsumerUpdate<'consumer> {
    consumer: &'consumer mut SearchProjectionConsumer,
    context: Option<PublicationContext>,
    prepared: Option<std::result::Result<PreparedRegisteredConsumer, Box<RegisteredConsumerError>>>,
}
impl ProjectionPreparedConsumerUpdate<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.context
            .as_ref()
            .expect("publication context")
            .dependencies
            .source_epoch()
    }
}
impl Drop for ProjectionPreparedConsumerUpdate<'_> {
    fn drop(&mut self) {
        match self.prepared.take() {
            Some(Ok(prepared)) => {
                // The exclusive Consumer borrow excludes another pending owner.
                let retained = self
                    .consumer
                    .projection_mut()
                    .retain_pending_publication(prepared);
                if let Err(error) = retained {
                    report_retention_failure(&error);
                }
            }
            Some(Err(error)) => {
                if let ProjectionConsumerCatchUpError::RecoveryRetention { retention, .. } =
                    retain_failed_owner(self.consumer, *error)
                {
                    report_retention_failure(&retention);
                }
            }
            None => {}
        }
    }
}

fn report_retention_failure(error: &HawDBError) {
    use std::io::Write;
    // Drop cannot return an error. Report the failed custody transfer in release
    // builds too, without a second panic if the host's diagnostic stream fails.
    let _ = writeln!(std::io::stderr().lock(), "HawDB Consumer recovery retention failed: {error}; durable birth and publication evidence retained");
}

/// Completed publication with deferred resident/source retirement. Return this
/// value from the database owner's callback and drop it after releasing the guard.
#[must_use]
pub struct ProjectionConsumerPublication {
    report: SearchProjectionConsumerCatchUpReport,
    _retired: Option<ConsumerProjection>,
    _context: PublicationContext,
}
impl ProjectionConsumerPublication {
    pub fn report(&self) -> &SearchProjectionConsumerCatchUpReport {
        &self.report
    }
    pub fn into_report(self) -> SearchProjectionConsumerCatchUpReport {
        self.report
    }
}

impl Database {
    /// Build and publish the already compared E contents through the stationary
    /// Consumer protocol, then acknowledge its durable native receipt. Final
    /// artifact construction is a blocking full-generation build. A lost reply
    /// uses `resolve_search_projection_consumer_publication` or verified reopen;
    /// it must not be treated as rollback or revalidated against a later E.
    pub fn publish_search_projection_consumer_covered_update(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
    ) -> CatchUpResult<SearchProjectionConsumerCatchUpReport> {
        self.publish_covered_consumer_with_limits(
            consumer,
            covered,
            durability,
            RegisteredConsumerLimits::default(),
        )
    }

    pub(in crate::api::search_projection_consumer) fn publish_covered_consumer_with_limits(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
        limits: RegisteredConsumerLimits,
    ) -> CatchUpResult<SearchProjectionConsumerCatchUpReport> {
        let preparation =
            self.reserve_covered_consumer_with_limits(consumer, covered, durability, limits)?;
        let prepared = preparation.prepare();
        Ok(self
            .publish_search_projection_consumer_prepared_update(prepared)?
            .into_report())
    }

    /// Reserve native custody under the owner, then release its guard before
    /// constructing the generation. The Consumer is exclusively borrowed until
    /// the prepared result is published or returned to its recovery state.
    pub fn prepare_search_projection_consumer_covered_update<'consumer>(
        &mut self,
        consumer: &'consumer mut SearchProjectionConsumer,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
    ) -> CatchUpResult<ProjectionConsumerPreparation<'consumer>> {
        self.reserve_covered_consumer_with_limits(
            consumer,
            covered,
            durability,
            RegisteredConsumerLimits::default(),
        )
    }

    fn reserve_covered_consumer_with_limits<'consumer>(
        &mut self,
        consumer: &'consumer mut SearchProjectionConsumer,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
        limits: RegisteredConsumerLimits,
    ) -> CatchUpResult<ProjectionConsumerPreparation<'consumer>> {
        let _descriptor_operation = self.internal_descriptor_operation()?;
        self.consumer_writable_root()?;
        self.reload_consumer_registry_if_unavailable()?;
        self.validate_consumer_receipt_at_retained_epoch(consumer, covered.source_epoch())?;
        let current = consumer.projection().receipt();
        let expires_at_commit_epoch = self
            .search_projection_consumer_status(consumer.id())?
            .expires_at_commit_epoch;
        if current != covered.base.receipt {
            return Err(HawDBError::TransactionConflict {
                read_epoch: covered.base.receipt.source_epoch,
                committed_epoch: current.source_epoch,
                key: "registered Consumer base or binding changed".into(),
            }
            .into());
        }
        let epoch = covered.source_epoch();
        self.validate_prepared_projection_source(&covered.dependencies, epoch, durability)?;
        // Task refusal before birth/intent leaves both selector and registry intact.
        covered
            .base
            .task
            .checkpoint()
            .map_err(ProjectionConsumerCatchUpError::Stopped)?;
        publication_failpoint(PublicationStage::BeforeCheckpoint)?;
        if epoch == current.source_epoch {
            return Ok(ProjectionConsumerPreparation {
                consumer: Some(consumer),
                context: Some(PublicationContext {
                    dependencies: covered.dependencies,
                    base: covered.base,
                    operations: 0,
                    durability,
                    expires_at_commit_epoch,
                }),
                construction: None,
            });
        }
        // Idle compared records retain no execution slot. Re-admit and settle
        // deterministic resident/reader refusals before reserving a birth.
        let permit = self.consumer_publication_permit()?;
        let documents = covered.documents.admit_publication(
            Arc::clone(&permit),
            covered.base.task.clone(),
            limits,
        )?;
        let recovery_memory = permit
            .reserve_retained_memory(
                hawdb_storage::consumer_allocation::CONSUMER_SELECTION_MEMORY_BOUND,
            )
            .map_err(|error| hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::SharedPool,
                owner: "covered Consumer recovery metadata".into(),
                memory_class: None,
                requested: error.requested,
                used: 0,
                budget: error.available,
                budget_option: "RuntimeGovernor.available_retained_memory_bytes",
                message: error.to_string(),
            })
            .map_err(HawDBError::from)?;
        let allocation = self
            .runtime
            .get()?
            .store
            .reserve_search_consumer_update(consumer.projection().stationary_receipt()?)?;
        let mut birth = consumer.projection().prepare_successor_birth(allocation)?;
        let locator = consumer.projection().registered_locator()?.to_owned();
        if let Err(error) = birth.complete_birth_intent() {
            return Err(retain_failed_owner(
                consumer,
                RegisteredConsumerError::with_recovery(
                    error,
                    RegisteredConsumerRecovery::unstarted(birth, locator, recovery_memory),
                ),
            ));
        }
        Ok(ProjectionConsumerPreparation {
            consumer: Some(consumer),
            context: Some(PublicationContext {
                dependencies: covered.dependencies,
                base: covered.base,
                operations: covered.operations,
                durability,
                expires_at_commit_epoch,
            }),
            construction: Some(Construction {
                documents,
                birth,
                locator,
                limits,
                recovery_memory,
                permit,
            }),
        })
    }

    /// Validate and select an already constructed generation. Construction and
    /// resident retirement are outside this database borrow. A lost reply still
    /// requires native resolution; source validation never rolls selection back.
    pub fn publish_search_projection_consumer_prepared_update(
        &mut self,
        mut update: ProjectionPreparedConsumerUpdate<'_>,
    ) -> CatchUpResult<ProjectionConsumerPublication> {
        let _descriptor_operation = self.internal_descriptor_operation()?;
        let root = self.consumer_writable_root()?;
        // The native pending operation deliberately hides this registration
        // from ordinary status/cache refresh. Its exact source/birth and previous
        // selector are fenced below; the exclusive borrow preserves the base.
        let context = update.context.as_ref().expect("publication context");
        let current = update.consumer.projection().receipt();
        if current != context.base.receipt {
            return Err(HawDBError::TransactionConflict {
                read_epoch: context.base.receipt.source_epoch,
                committed_epoch: current.source_epoch,
                key: "registered Consumer base or binding changed".into(),
            }
            .into());
        }
        let epoch = context.dependencies.source_epoch();
        let source = self
            .runtime
            .get()?
            .store
            .search_projection_changefeed_status();
        if source.graph_commit_epoch >= context.expires_at_commit_epoch {
            return Err(Error::RebuildRequired(Reason::Expired {
                expires_at_commit_epoch: context.expires_at_commit_epoch,
            })
            .into());
        }
        if epoch < source.resume_floor_commit_epoch {
            return Err(Error::RebuildRequired(Reason::RetentionLimitExceeded {
                resume_floor_commit_epoch: source.resume_floor_commit_epoch,
            })
            .into());
        }
        self.validate_prepared_projection_source(&context.dependencies, epoch, context.durability)?;
        context
            .base
            .task
            .checkpoint()
            .map_err(ProjectionConsumerCatchUpError::Stopped)?;
        let permit = self.consumer_publication_permit()?;
        let Some(preparation) = update.prepared.take() else {
            let report = self.covered_consumer_report(update.consumer, epoch, epoch, 0, 0)?;
            return Ok(ProjectionConsumerPublication {
                report,
                _retired: None,
                _context: update.context.take().expect("publication context"),
            });
        };
        let consumer = &mut *update.consumer;
        let mut prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => return Err(retain_failed_owner(consumer, *error)),
        };
        let selection = (|| -> CatchUpResult<_> {
            publication_failpoint(PublicationStage::AfterCheckpoint)?;
            self.validate_prepared_projection_source(
                &context.dependencies,
                epoch,
                context.durability,
            )?;
            let runtime = self.runtime.get()?;
            runtime.store.verify_search_consumer_candidate_source(
                prepared.reservation_binding()?,
                epoch,
                prepared.report().metadata_reference,
            )?;
            let mut attempt =
                prepared.admit_selection(Arc::clone(&permit), context.base.task.clone())?;
            publication_failpoint(PublicationStage::BeforeSelector)?;
            let pending = attempt.select()?;
            drop(runtime);
            publication_failpoint(PublicationStage::AfterSelector)?;
            let receipt = pending.finish()?;
            publication_failpoint(PublicationStage::AfterSelectorBarrier)?;
            Ok(receipt)
        })();
        let selected = match selection {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Err(retention) = consumer
                    .projection_mut()
                    .retain_pending_publication(prepared)
                {
                    return Err(ProjectionConsumerCatchUpError::RecoveryRetention {
                        failure: Box::new(error),
                        retention,
                    });
                }
                return Err(error);
            }
        };
        let retired = match prepared.complete(selected) {
            Ok(projection) => std::mem::replace(consumer.projection_mut(), projection),
            Err(error) => return Err(retain_failed_owner(consumer, *error)),
        };
        let receipt = consumer.projection().receipt();
        self.reload_consumer_registry_if_unavailable()?;
        self.acknowledge_consumer_checkpoint(consumer.id(), &receipt)?;
        self.publish_consumer_registry(&root)?;
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(consumer.id().as_str().into(), State::Active);
        let report = self.covered_consumer_report(
            consumer,
            current.source_epoch,
            receipt.source_epoch,
            1,
            context.operations,
        )?;
        Ok(ProjectionConsumerPublication {
            report,
            _retired: Some(retired),
            _context: update.context.take().expect("publication context"),
        })
    }

    fn covered_consumer_report(
        &self,
        consumer: &SearchProjectionConsumer,
        start: u64,
        end: u64,
        publications: usize,
        operations: usize,
    ) -> CatchUpResult<SearchProjectionConsumerCatchUpReport> {
        Ok(SearchProjectionConsumerCatchUpReport {
            catch_up: SearchProjectionCatchUpReport {
                graph_commit_epoch: end,
                start_applied_epoch: Some(start),
                start_durable_epoch: Some(start),
                end_applied_epoch: Some(end),
                end_durable_epoch: Some(end),
                applied_batch_count: publications,
                applied_operation_count: operations,
                complete: true,
            },
            consumer: self.search_projection_consumer_status(consumer.id())?,
        })
    }
}

fn retain_failed_owner(
    consumer: &mut SearchProjectionConsumer,
    error: RegisteredConsumerError,
) -> ProjectionConsumerCatchUpError {
    let (error, recovery) = error.into_parts();
    let failure = ProjectionConsumerCatchUpError::from(error);
    if let Some(recovery) = recovery
        && let Err(retention) = consumer
            .projection_mut()
            .retain_failed_preparation(recovery)
    {
        return ProjectionConsumerCatchUpError::RecoveryRetention {
            failure: Box::new(failure),
            retention,
        };
    }
    failure
}
