//! The engine fixture: what the shared connector suites need from an engine.
//!
//! Each engine implements [`ConnectorTestEngine`] once, and the format, transport, and
//! connector suites reach the engine only through it, so that one copy of each suite checks
//! every engine. A suite describes data as a [`Relation`] and Arrow batches in the published
//! form, and compares results as a [`ZSet`].

use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use feldera_types::program_schema::Relation;

use crate::catalog::{InputCollectionHandle, SerBatchReader};
use crate::test::{Updates, ZSet, default_timeout_ms, wait};

/// Engine-specific functionality used by shared connector tests.
pub trait ConnectorTestEngine: Send + Sync {
    /// Returns the engine's name, for test output.
    fn name(&self) -> &'static str;

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
    timeout_ms: u128,
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
    /// The collection takes a deleted record as an insert of the record with the connector
    /// attribute `is_delete` set, as a table with soft deletes does.
    pub soft_delete: bool,

    /// Columns whose DEFAULT reads a connector attribute: (column, attribute) pairs.
    pub attribute_defaults: Vec<(String, String)>,
}
