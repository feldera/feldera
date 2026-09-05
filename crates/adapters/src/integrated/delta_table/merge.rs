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
//! | [`startup`] | What the target table must satisfy before the first row moves |
//! | [`flush`] | The walk that drives all of the above and commits the result |
//! | [`metrics`] | What the connector reports, and when it says the table needs compacting |

pub(crate) mod chunk;
pub(crate) mod flush;
pub(crate) mod key;
pub(crate) mod metrics;
pub(crate) mod probe;
pub(crate) mod prune;
pub(crate) mod startup;
pub(crate) mod tombstone;

#[cfg(test)]
mod model;

#[cfg(test)]
mod test;
