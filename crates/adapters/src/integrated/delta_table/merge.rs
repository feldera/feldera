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

pub(crate) mod chunk;
pub(crate) mod key;
