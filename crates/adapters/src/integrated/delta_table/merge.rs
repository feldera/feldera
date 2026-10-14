//! Merge mode: keep a Delta table in sync with the contents of a view.
//!
//! Instead of appending a change log, the connector appends new row versions and
//! tombstones superseded ones with Delta deletion vectors, so a flush never rewrites a
//! data file. See `docs/design/delta_merge_mode.md`.
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

use super::WriteError;
use anyhow::{Result as AnyResult, anyhow};
use deltalake::kernel::transaction::{
    CommitBuilder, CommitProperties, TableReference, TransactionError,
};
use deltalake::kernel::{Action, DeletionVectorDescriptor};
use deltalake::protocol::DeltaOperation;
use deltalake::{DeltaTable, DeltaTableError};
use feldera_adapterlib::utils::backoff::calculate_backoff_delay;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use tokio::time::sleep;
use tracing::warn;

/// How many times one object-store request is attempted before the flush gives up on it.
///
/// Bounded, unlike the connector's `max_retries`: this loop sits inside that one, and two
/// nested unbounded loops would make a shutdown wait for both.  Four attempts spend about
/// 3.5s of backoff, enough to ride out a throttled or dropped request.
const IO_ATTEMPTS: u32 = 4;

/// Run `operation` until it succeeds, fails deterministically, or [`IO_ATTEMPTS`] attempts
/// have failed.
///
/// The flush's own retry is the whole flush, which redoes every lookup; that is far too
/// much to pay for one flaky request, and delta-rs and `object_store` do not reliably
/// retry on their own.  A failure that survives this stays [`WriteError::Transient`],
/// leaving the decision to retry the flush to the caller.
///
/// `description` completes the sentence "error ...", and the endpoint it belongs to comes
/// from the caller's tracing span.
pub(crate) async fn retry_io<F, Fut, T>(
    description: &str,
    mut operation: F,
) -> Result<T, WriteError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, WriteError>>,
{
    for attempt in 1..=IO_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            // Retrying cannot change a deterministic outcome, and the caller must see it
            // unchanged or it would retry the whole flush for ever on bad data.
            Err(e @ WriteError::Deterministic(_)) => return Err(e),
            Err(e) if attempt == IO_ATTEMPTS => {
                return Err(WriteError::Transient(anyhow!(
                    "error {description} after {attempt} attempts: {e}"
                )));
            }
            Err(e) => {
                let backoff = calculate_backoff_delay(attempt - 1);
                warn!("error {description} (attempt {attempt}, retrying in {backoff:?}): {e}");
                sleep(backoff).await;
            }
        }
    }
    unreachable!("the loop returns on the last attempt")
}

/// An object-store failure, which is always worth another attempt.
pub(crate) fn transient(e: impl Display) -> WriteError {
    WriteError::Transient(anyhow!("{e}"))
}

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

/// How often a commit moves onto a newer table version before the caller redoes its work.
const COMMIT_ATTEMPTS: usize = 5;

/// Commit `actions` and advance `table` to the committed version.
///
/// Never trusts delta-rs's conflict checker: since delta-rs#4568 it lets a remove commit over
/// an `OPTIMIZE` that already removed the same file, which revives the file and duplicates
/// its rows. A commit lands only on the version it read, or on a newer one when
/// [`first_stale_remove`] finds nothing it depends on changed.
pub(crate) async fn commit_actions(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> AnyResult<()> {
    for _ in 0..COMMIT_ATTEMPTS {
        let read_snapshot = table.state.as_ref().map(|s| s as &dyn TableReference);
        let committed = CommitBuilder::from(CommitProperties::default().with_max_retries(0))
            .with_actions(actions.clone())
            .build(read_snapshot, table.log_store(), operation.clone())
            .await;
        match committed {
            Ok(finalized) => {
                // The next flush looks up rows in this snapshot, so it must include what this
                // commit wrote; otherwise an update to a row inserted here would duplicate it.
                table.state = Some(finalized.snapshot);
                return Ok(());
            }
            // `max_retries(0)` reports a newer version this way.
            Err(DeltaTableError::Transaction {
                source: TransactionError::MaxCommitAttempts(_),
            }) => {
                let read = table.snapshot()?;
                let (metadata, protocol) = (read.metadata().clone(), read.protocol().clone());
                table.update_state().await?;
                let latest = table.snapshot()?;
                if latest.metadata() != &metadata || latest.protocol() != &protocol {
                    return Err(anyhow!(
                        "a concurrent commit changed the Delta table's schema, properties, or \
                         protocol since this commit read it"
                    ));
                }
                // Our own attempt may have landed with only its response lost. Its files have
                // fresh names, so finding one live proves it, and committing again would log
                // every add twice for a streaming reader.
                if already_committed(table, &actions)? {
                    return Ok(());
                }
                if let Some(path) = first_stale_remove(table, &actions)? {
                    return Err(anyhow!(
                        "a concurrent commit (for example OPTIMIZE) removed or rewrote '{path}', \
                         which this commit also changes"
                    ));
                }
            }
            Err(e) => {
                return Err(anyhow!(
                    "error committing to the Delta table (read version: {:?}): {e:?}",
                    table.version()
                ));
            }
        }
    }
    Err(anyhow!(
        "the Delta table gained a newer version on each of {COMMIT_ATTEMPTS} attempts to commit"
    ))
}

/// Whether `table` already holds the first file `actions` adds, with the same deletion vector.
fn already_committed(table: &DeltaTable, actions: &[Action]) -> AnyResult<bool> {
    let Some(add) = actions.iter().find_map(|action| match action {
        Action::Add(add) => Some(add),
        _ => None,
    }) else {
        return Ok(false);
    };
    Ok(table.snapshot()?.log_data().into_iter().any(|file| {
        file.path() == add.path.as_str() && file.deletion_vector_descriptor() == add.deletion_vector
    }))
}

/// The first file `actions` removes that `table` no longer holds as the remove saw it: gone,
/// or carrying a different deletion vector. `None` when every remove still applies.
fn first_stale_remove(table: &DeltaTable, actions: &[Action]) -> AnyResult<Option<String>> {
    let mut expected: HashMap<&str, &Option<DeletionVectorDescriptor>> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Remove(remove) => Some((remove.path.as_str(), &remove.deletion_vector)),
            _ => None,
        })
        .collect();
    for file in table.snapshot()?.log_data() {
        let path = file.path();
        if let Some(seen) = expected.get(path.as_ref())
            && &file.deletion_vector_descriptor() == *seen
        {
            expected.remove(path.as_ref());
        }
    }
    Ok(expected.into_keys().next().map(str::to_string))
}

#[cfg(test)]
mod model;

#[cfg(test)]
mod test;

#[cfg(test)]
mod retry_test {
    use super::*;
    use std::cell::Cell;

    /// Run `op` through [`retry_io`], returning its outcome and how often it was called.
    async fn attempts<T>(
        op: impl Fn(u32) -> Result<T, WriteError>,
    ) -> (Result<T, WriteError>, u32) {
        let calls = Cell::new(0);
        let result = retry_io("in a test", || {
            calls.set(calls.get() + 1);
            let outcome = op(calls.get());
            async move { outcome }
        })
        .await;
        (result, calls.get())
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried() {
        let (result, calls) = attempts(|call| match call {
            1 => Err(transient("the object store hiccuped")),
            _ => Ok(()),
        })
        .await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn a_deterministic_failure_is_not_retried() {
        // The point of the classification: bad data must not spin the connector.
        let (result, calls) = attempts(|_| {
            Err::<(), _>(WriteError::Deterministic(anyhow!(
                "the schema does not match"
            )))
        })
        .await;
        assert!(
            matches!(result, Err(WriteError::Deterministic(_))),
            "{result:?}"
        );
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn a_persistent_failure_gives_up_and_stays_transient() {
        // Still transient, so the caller may retry the whole flush; only this loop is spent.
        let (result, calls) =
            attempts(|_| Err::<(), _>(transient("the object store is down"))).await;
        assert!(
            matches!(result, Err(WriteError::Transient(_))),
            "{result:?}"
        );
        assert_eq!(calls, IO_ATTEMPTS);
    }
}
