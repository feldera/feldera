//! What a circuit restored from a checkpoint must rebuild after its program changed, and how
//! the bootstrap that rebuilds it started.

use std::collections::BTreeSet;

use feldera_types::program_schema::SqlIdentifier;

/// What a circuit restored from a checkpoint must rebuild, because its program changed since
/// the checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct BootstrapInfo {
    /// The input tables whose state the circuit rebuilds. Their connectors start over instead of
    /// resuming from their checkpointed offsets.
    pub backfill_tables: BTreeSet<SqlIdentifier>,

    /// The persistent ids of all the operators that the circuit rebuilds.
    pub backfill_persistent_ids: BTreeSet<String>,
}

/// How a concurrent bootstrap started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConcurrentRestoreOutcome {
    /// The checkpoint matches the circuit: there is nothing to bootstrap, and the circuit is
    /// fully active.
    UpToDate,
    /// The circuit serves its unchanged part while a separate bootstrap circuit rebuilds the
    /// rest.
    Concurrent(BootstrapInfo),
    /// The rebuilt part cannot run concurrently, so the circuit fell back to a bootstrap that
    /// stops it until the bootstrap finishes.
    FellBack {
        /// Why the concurrent bootstrap was refused.
        reason: String,
        /// What the fallback bootstrap rebuilds.
        info: Option<BootstrapInfo>,
    },
}
