//! Merge mode: keep a Delta table in sync with the contents of a view.
//!
//! Instead of appending a change log, the connector appends new row versions and
//! tombstones superseded ones with Delta deletion vectors, so no data file is ever
//! rewritten. See `docs/design/delta_merge_mode.md`.
//!
//! Module layout mirrors the stages of one flush:
//!
//! | Module | Role |
//! |--------|------|
//! | [`key`] | Which types may form a key, and how key values become comparable bytes |
//! | [`chunk`] | The bounded buffer of encoded keys awaiting a lookup pass |
//! | [`prune`] | Which files and row groups the lookup can skip without reading them |
//! | [`probe`] | Turning a set of keys into the (file, row ordinal) pairs to tombstone |
//! | [`tombstone`] | Turning located row ordinals into deletion vectors and log actions |
//! | [`rewrite`] | Reclaiming storage from files whose rows are mostly superseded |
//! | [`startup`] | What the target table must satisfy before the first row moves |
//! | [`flush`] | The walk that drives all of the above and commits the result |
//! | [`compact`] | Optional connector-driven OPTIMIZE, for tables nothing else maintains |
//! | [`metrics`] | What the connector reports, and when it says the table needs compacting |

use anyhow::{Result as AnyResult, anyhow};
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::protocol::DeltaOperation;

pub(crate) mod chunk;
pub(crate) mod compact;
pub(crate) mod flush;
pub(crate) mod key;
pub(crate) mod metrics;
pub(crate) mod probe;
pub(crate) mod prune;
pub(crate) mod rewrite;
pub(crate) mod startup;
pub(crate) mod tombstone;

/// Commit `actions` and advance `table` to the committed version.
///
/// The table is deliberately *not* refreshed first: keeping the caller's snapshot as the
/// commit's read version is what makes a concurrent change to the files it addressed come
/// back as a conflict rather than silently winning. Both writers depend on that, so they
/// share one place that gets it right.
pub(crate) async fn commit_actions(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> AnyResult<()> {
    let read_snapshot = table.state.as_ref().map(|s| s as &dyn TableReference);

    let finalized = CommitBuilder::from(CommitProperties::default())
        .with_actions(actions)
        .build(read_snapshot, table.log_store(), operation)
        .await
        .map_err(|e| {
            anyhow!(
                "error committing to the Delta table (read version: {:?}): {e:?}",
                table.version()
            )
        })?;

    // The next flush looks up rows in this snapshot, so it must include what this commit
    // wrote; otherwise an update to a row inserted here would duplicate it.
    table.state = Some(finalized.snapshot);
    Ok(())
}

#[cfg(test)]
mod model;

#[cfg(test)]
mod test;
