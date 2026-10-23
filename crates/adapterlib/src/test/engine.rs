//! The engine fixture: what the shared connector suites need from an engine.
//!
//! Each engine implements [`ConnectorTestEngine`] once, and the format, transport, and
//! connector suites reach the engine only through it, so that one copy of each suite checks
//! every engine. A suite describes data as a [`Relation`] and Arrow batches in the published
//! form, and compares results as a [`ZSet`].

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use feldera_types::program_schema::Relation;

use crate::catalog::{InputCollectionHandle, SerBatchReader};
use crate::test::{Updates, ZSet, default_timeout_ms, wait};

/// What a shared suite case needs from an engine beyond the methods of the fixture.
///
/// A case names its capabilities with `#[engine_test(requires(...))]`, and the harness ignores
/// the case on an engine that does not have one of them ([`ConnectorTestEngine::has`]). The
/// capabilities from [`Capability::JsonArrayRecords`] on are behaviors where the engines differ:
/// a case that checks one of them runs only on an engine that behaves that way. A new
/// capability goes into [`Capability::ALL`] too.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    /// A pipeline with storage.
    Storage,
    /// A pipeline takes checkpoints, lists them, or starts from one.
    Checkpoints,
    /// A pipeline suspends to its storage and resumes.
    SuspendResume,
    /// A fault-tolerant pipeline replays its input after a restart.
    FaultTolerance,
    /// A pipeline bootstraps the changed part of a modified program.
    Bootstrap,
    /// The `raw` input format.
    RawFormat,
    /// Rows rendered for the Postgres output connector.
    PostgresRendering,
    /// Rows rendered as DynamoDB items.
    DynamoDbItems,
    /// The circuit's JSON and graph profiles.
    Profiles,
    /// The circuit's low-level IR, which `GET /lir` serves.
    Lir,
    /// A JSON record written as an array of positional values, such as `[true, 0, "a"]`.
    JsonArrayRecords,
    /// serde's text for a record that does not fit its table, which names the line, the
    /// column, and the field.
    SerdeParseErrors,
    /// A binary VARIANT value in JSON as an array of bytes, not as base64 text.
    VariantBinaryAsArray,
    /// A `-0.0` key kept as `-0.0`, not as `0.0`.
    NegativeZeroKeys,
    /// The storage files of the static engine, such as `checkpoints.feldera`.
    StaticStorageLayout,
    /// The controller's clock fills the table `now`, so that `NOW()` follows the wall clock.
    WallClock,
    /// A commit feeds the transaction's input to the circuit in chunks of
    /// `dev_tweaks.splitter_chunk_size_records` records, one chunk per step, so that the commit
    /// of a larger transaction takes several steps.
    ChunkedCommit,
}

impl Capability {
    /// Every capability, in the order of declaration.
    pub const ALL: [Capability; 17] = [
        Capability::Storage,
        Capability::Checkpoints,
        Capability::SuspendResume,
        Capability::FaultTolerance,
        Capability::Bootstrap,
        Capability::RawFormat,
        Capability::PostgresRendering,
        Capability::DynamoDbItems,
        Capability::Profiles,
        Capability::Lir,
        Capability::JsonArrayRecords,
        Capability::SerdeParseErrors,
        Capability::VariantBinaryAsArray,
        Capability::NegativeZeroKeys,
        Capability::StaticStorageLayout,
        Capability::WallClock,
        Capability::ChunkedCommit,
    ];
}

/// Engine-specific functionality used by shared connector tests.
pub trait ConnectorTestEngine: Send + Sync {
    /// Returns the engine's name, for test output.
    fn name(&self) -> &'static str;

    /// Returns `true` if the engine has `capability`. The harness ignores a case that requires
    /// a capability that the engine does not have.
    fn has(&self, _capability: Capability) -> bool {
        true
    }

    /// Returns `true` if the harness ignores a case whose service, such as a Kafka broker, does
    /// not answer at the address that the case reads from the environment. By default such a
    /// case runs and fails, so that a run without the service does not pass unnoticed.
    fn skip_absent_services(&self) -> bool {
        false
    }

    /// Returns an input collection for `relation` that records what parsers flush into it.
    ///
    /// Fails if the engine does not support `relation` with `options`.
    ///
    /// The static engine must precompile a fixed set of relation types, and only those
    /// relations are available at runtime.
    fn input_collection(
        &self,
        relation: &Relation,
        options: &InputOptions,
    ) -> AnyResult<Box<dyn TestCollection>>;

    /// Returns `rows`, each with the weight at its index in `weights`, as one consolidated
    /// output batch of `relation`.
    ///
    /// Fails if the engine does not support `relation`, or if `rows` is not in the published
    /// form of `relation`.
    fn output_batch(
        &self,
        relation: &Relation,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>>;

    /// Returns `rows` of `relation`, each with the weight at its index in `weights`, as one
    /// consolidated output batch of the index `key` on `relation`: each row is a value under
    /// the key that its columns in `key` hold.
    ///
    /// Fails if the engine does not support the index, or if `rows` is not in the published
    /// form of `relation`.
    fn indexed_output_batch(
        &self,
        key: &Relation,
        relation: &Relation,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>>;
}

/// An input collection that records the rows that parsers flush into it.
pub trait TestCollection: Send + Sync {
    /// Returns the handle to build a parser for the collection with.
    fn input_handle(&self) -> InputCollectionHandle;

    /// Returns the published Arrow schema of the collection's rows.
    fn schema(&self) -> SchemaRef;

    /// Returns the rows flushed since the last [`Self::reset`], in the order of arrival, in the
    /// published form, followed by a [`WEIGHT_COLUMN`](crate::test::WEIGHT_COLUMN).
    fn batches(&self) -> AnyResult<Vec<RecordBatch>>;

    /// Returns the number of rows flushed since the last [`Self::reset`].
    fn len(&self) -> usize;

    /// Returns `true` if no rows were flushed since the last [`Self::reset`].
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forgets the rows flushed so far.
    fn reset(&self);

    /// Returns the rows flushed since the last [`Self::reset`], in the order of arrival.
    fn updates(&self) -> AnyResult<Updates> {
        Updates::from_weighted_batches(self.schema(), &self.batches()?)
    }

    /// Returns the rows flushed since the last [`Self::reset`], consolidated.
    fn zset(&self) -> AnyResult<ZSet> {
        self.updates()?.zset()
    }
}

/// Waits until `collection` holds at least `count` rows, calling `flush` before each check,
/// and returns its rows in the order of arrival.
///
/// Panics if the rows do not arrive within [`default_timeout_ms`].
pub fn wait_for_rows(
    collection: &dyn TestCollection,
    count: usize,
    flush: impl FnMut(),
) -> Updates {
    wait_for_rows_within(collection, count, default_timeout_ms(), flush)
}

/// Like [`wait_for_rows`], but panics if the rows do not arrive within `timeout_ms`.
pub fn wait_for_rows_within(
    collection: &dyn TestCollection,
    count: usize,
    timeout_ms: u64,
    mut flush: impl FnMut(),
) -> Updates {
    wait(
        || {
            flush();
            collection.len() >= count
        },
        timeout_ms,
    )
    .unwrap_or_else(|()| {
        panic!(
            "waited {timeout_ms} ms for {count} rows, but {} arrived",
            collection.len()
        )
    });
    collection.updates().unwrap()
}

/// How a suite configures an input collection.
#[derive(Clone, Debug, Default)]
pub struct InputOptions {
    /// Converts deletions into insertions while setting the `is_delete` connector metadata
    /// attribute to `true`, as a table with soft deletes does.
    pub soft_delete: bool,

    /// The connector attribute that the DEFAULT of a column reads, by column.
    pub attribute_defaults: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{Result as AnyResult, bail};
    use arrow::array::RecordBatch;
    use feldera_types::program_schema::Relation;

    use super::{Capability, ConnectorTestEngine, InputOptions, TestCollection};
    use crate::catalog::SerBatchReader;

    /// An engine that implements only the methods that the trait requires.
    struct BareEngine;

    impl ConnectorTestEngine for BareEngine {
        fn name(&self) -> &'static str {
            "bare"
        }

        fn input_collection(
            &self,
            _relation: &Relation,
            _options: &InputOptions,
        ) -> AnyResult<Box<dyn TestCollection>> {
            bail!("no collections")
        }

        fn output_batch(
            &self,
            _relation: &Relation,
            _rows: &RecordBatch,
            _weights: &[i64],
        ) -> AnyResult<Arc<dyn SerBatchReader>> {
            bail!("no batches")
        }

        fn indexed_output_batch(
            &self,
            _key: &Relation,
            _relation: &Relation,
            _rows: &RecordBatch,
            _weights: &[i64],
        ) -> AnyResult<Arc<dyn SerBatchReader>> {
            bail!("no batches")
        }
    }

    /// An engine that does not say otherwise has every capability and lets a case whose
    /// service is absent fail, so that it runs every case.
    #[test]
    fn an_engine_has_every_capability_by_default() {
        for capability in Capability::ALL {
            assert!(BareEngine.has(capability), "{capability:?}");
        }
        assert!(!BareEngine.skip_absent_services());
    }

    /// `Capability::ALL` lists each capability once, in the order of declaration.
    #[test]
    fn all_lists_each_capability_in_order() {
        for (index, capability) in Capability::ALL.into_iter().enumerate() {
            assert_eq!(capability as usize, index, "{capability:?}");
        }
    }
}
