use super::*;

/// What startup does with etl's stored state for the source table; see
/// [`reconcile_table_state`].
#[derive(Debug, PartialEq)]
pub(super) enum Reconcile {
    /// An error a restart makes moot: roll the state back to what preceded it.
    RollBack,
    /// The table sync is complete but no checkpoint holds the copy: read it
    /// again.
    RedoCopy,
    /// etl completed a copy after the checkpoint the pipeline resumes from,
    /// so that copy's rows are in neither the checkpoint nor the events etl
    /// replays: read the table again.
    RedoNewerCopy,
    /// The table sync is complete and the circuit holds the copy.
    SyncCompleted,
    /// etl still has the copy to make or to finish, or an error only etl or
    /// the user can clear.
    CopyOutstanding,
}

/// What the checkpoint a start resumes from says about the table's copy.
///
/// A start with no checkpoint at all is not the same as one whose checkpoint
/// records no copy: the first has nothing to compare a copy against, while the
/// second holds a copy older than any etl reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResumedCopy {
    /// The start has no checkpoint to resume from.
    NoCheckpoint,
    /// A checkpoint written before the connector recorded copy positions, so
    /// it predates every copy etl has completed.
    CheckpointWithoutLsn,
    /// A checkpoint that recorded the copy etl completed at this LSN.
    CheckpointAt(u64),
}

impl ResumedCopy {
    /// What a start resuming from `resume_info` knows about the copy.
    pub(super) fn of(resume_info: Option<&Value>) -> Self {
        match resume_info {
            None => Self::NoCheckpoint,
            Some(info) => match info.get(COPY_SYNC_LSN_KEY).and_then(Value::as_u64) {
                None => Self::CheckpointWithoutLsn,
                Some(lsn) => Self::CheckpointAt(lsn),
            },
        }
    }

    /// The copy position the checkpoint recorded, if it recorded one.
    pub(super) fn lsn(&self) -> Option<u64> {
        match self {
            Self::CheckpointAt(lsn) => Some(*lsn),
            _ => None,
        }
    }

    /// Whether a copy etl completed at `lsn` is newer than what the checkpoint
    /// holds. A start with no checkpoint has nothing to be newer than: it
    /// either reads the table again because fault tolerance asks for a
    /// complete circuit, or it streams from the slot and leaves the table's
    /// earlier rows out of Feldera, which the connector documents.
    fn predates(&self, lsn: u64) -> bool {
        match self {
            Self::NoCheckpoint => false,
            Self::CheckpointWithoutLsn => true,
            Self::CheckpointAt(recorded) => lsn > *recorded,
        }
    }
}

pub(super) fn reconcile_decision(
    state: &TableState,
    redo_snapshot: bool,
    resumed_copy: ResumedCopy,
) -> Reconcile {
    if let TableState::Errored {
        reason,
        retry_policy,
        ..
    } = state
        && (is_legacy_stop_error(reason)
            || matches!(retry_policy, TableRetryPolicy::TimedRetry { .. }))
    {
        return Reconcile::RollBack;
    }
    if !sync_completed(state) {
        return Reconcile::CopyOutstanding;
    }
    if redo_snapshot {
        return Reconcile::RedoCopy;
    }
    // etl skips a `sync_done` table's events below its LSN, because the copy
    // at that LSN is meant to hold them. A checkpoint that predates the copy
    // holds neither, so its rows would be lost for good. `Ready` needs no
    // check under fault tolerance: etl reaches it only once its persisted
    // progress has passed the LSN, and that progress advances only past
    // checkpointed steps, so a checkpoint holding the copy exists. Without
    // fault tolerance etl's progress runs ahead of any checkpoint, and a
    // resume after a crash loses the changes since the checkpoint anyway;
    // the copy is one more thing that resume cannot recover, and the docs
    // say so.
    if let TableState::SyncDone { lsn, .. } = state
        && resumed_copy.predates(u64::from(*lsn))
    {
        return Reconcile::RedoNewerCopy;
    }
    Reconcile::SyncCompleted
}

/// Bring etl's persisted state for `source_table` in line with what Feldera
/// has durably ingested, before etl starts, and say whether the connector
/// still owes the circuit a copy of the table.
///
/// 1. Roll back errors that a restart makes moot: errors etl recorded because
///    Feldera terminated the connector mid-batch, whose writes were never
///    acknowledged and are safe to replay, and timed retries, whose deadline
///    etl does not honor across a restart (it never respawns the worker of
///    an `Errored` table, so the table would stall for good). A rollback that
///    fails resets the table to `Init` and reads it again: that clears the
///    error at the cost of a copy, where failing the start would need the
///    state edited by hand.
/// 2. With `redo_snapshot`, reset a completed table sync to `Init`. The circuit
///    starts empty because there is no checkpoint to resume from, whereas etl
///    would skip the copy and stream from the slot. Steps report a barrier
///    until the copy is in the circuit, so this is the only case where a
///    resumed pipeline can be missing it.
///
/// Roll `table_id` back from `state`, an error a stop left behind, to the
/// state before it, and return that state.
///
/// etl deletes the row it rolls back from, so a table whose history holds
/// only the error has nothing to go back to. Such a table is read again
/// instead, reported as `None`: that clears the error and costs a copy, where
/// failing the start would need the state edited by hand and leaving it would
/// stall the table for good.
pub(super) async fn roll_back_stop_error(
    store: &PostgresStore,
    table_id: TableId,
    state: &TableState,
    endpoint_name: &str,
) -> EtlResult<Option<TableState>> {
    let rolled_back = format!("{state:?}");
    match store.rollback_table_state(table_id).await {
        Ok(previous) => {
            info!(
                "postgres_cdc {endpoint_name}: rolled back {rolled_back} for table {table_id}; \
                 resuming from state {previous:?}"
            );
            Ok(Some(previous))
        }
        Err(e) => {
            warn!(
                "postgres_cdc {endpoint_name}: cannot roll back {rolled_back} for table \
                 {table_id} ({e}); reading the table again"
            );
            store.update_table_state(table_id, TableState::Init).await?;
            Ok(None)
        }
    }
}

/// Clear the stop errors an older connector left on the other tables of the
/// publication, before etl starts.
///
/// etl copies every table of the publication under this pipeline, and the
/// older connector refused the write of any table at a stop, so a table other
/// than the source table can carry such an error too. etl never retries a
/// `ManualRetry` error on its own, and the monitor reports an error on any
/// table as fatal, so left in place it would stop the connector at every
/// start. The source table's own state is reconciled by its caller.
pub(super) async fn roll_back_legacy_stop_errors(
    store: &PostgresStore,
    source_table_id: Option<TableId>,
    endpoint_name: &str,
) -> EtlResult<()> {
    let states = store.get_table_states().await?;
    for (table_id, state) in states.iter() {
        if Some(*table_id) == source_table_id {
            continue;
        }
        if let TableState::Errored { reason, .. } = state
            && is_legacy_stop_error(reason)
        {
            roll_back_stop_error(store, *table_id, state, endpoint_name).await?;
        }
    }
    Ok(())
}

/// What startup found out about the source table.
pub(super) struct Reconciled {
    /// etl still has the source table to copy, which is what the connector
    /// holds its checkpoint barrier for until the copy is in the circuit.
    pub(super) copy_outstanding: bool,
    /// The `sync_done` LSN etl holds for the table, when its sync is
    /// complete, so the barrier knows it before the monitor's first poll.
    pub(super) sync_done_lsn: Option<u64>,
}

pub(super) async fn reconcile_table_state(
    store: &PostgresStore,
    source_table: &str,
    redo_snapshot: bool,
    resumed_copy: ResumedCopy,
    endpoint_name: &str,
) -> EtlResult<Reconciled> {
    let outstanding = Reconciled {
        copy_outstanding: true,
        sync_done_lsn: None,
    };
    store.load_cache().await?;
    let table_id = target_table_id(store, source_table).await?;
    roll_back_legacy_stop_errors(store, table_id, endpoint_name).await?;
    let Some(table_id) = table_id else {
        return Ok(outstanding);
    };
    let Some(mut state) = store.get_table_state(table_id).await? else {
        return Ok(outstanding);
    };

    loop {
        match reconcile_decision(&state, redo_snapshot, resumed_copy) {
            Reconcile::RollBack => {
                match roll_back_stop_error(store, table_id, &state, endpoint_name).await? {
                    Some(previous) => state = previous,
                    None => return Ok(outstanding),
                }
            }
            Reconcile::RedoCopy => {
                warn!(
                    "postgres_cdc {endpoint_name}: no checkpoint holds the initial copy of \
                     table {table_id}; reading it again"
                );
                store.update_table_state(table_id, TableState::Init).await?;
                return Ok(outstanding);
            }
            Reconcile::RedoNewerCopy => {
                warn!(
                    "postgres_cdc {endpoint_name}: etl completed a copy of table {table_id} \
                     after the checkpoint the pipeline resumes from ({state:?}, checkpoint \
                     recorded {resumed_copy:?}); reading it again"
                );
                store.update_table_state(table_id, TableState::Init).await?;
                return Ok(outstanding);
            }
            Reconcile::SyncCompleted => {
                let sync_done_lsn = match &state {
                    TableState::SyncDone { lsn, .. } => Some(u64::from(*lsn)),
                    _ => None,
                };
                return Ok(Reconciled {
                    copy_outstanding: false,
                    sync_done_lsn,
                });
            }
            Reconcile::CopyOutstanding => return Ok(outstanding),
        }
    }
}
