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

use super::WriteError;
use anyhow::{Result as AnyResult, anyhow};
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::protocol::DeltaOperation;
use feldera_adapterlib::utils::backoff::calculate_backoff_delay;
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
