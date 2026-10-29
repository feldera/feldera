//! The pipeline fixture: what the suites that run whole pipelines need from an engine.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::array::RecordBatch;
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_adapterlib::test::ConnectorTestEngine;
use feldera_storage::tokio::TOKIO;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use uuid::Uuid;

use crate::engine::{CircuitFactory, OpenStorageFn};
use crate::{Controller, ControllerError, PipelineConfig};

/// The callback that a test controller reports errors to: the error, and the tag of the
/// endpoint that failed, if any.
pub type ControllerErrorCallback = Box<dyn Fn(Arc<ControllerError>, Option<String>) + Send + Sync>;

/// Registers what a pipeline needs in the catalog of its circuit, such as the preprocessors
/// and postprocessors that its configuration names, before the controller opens the
/// connectors.
pub type CatalogSetup = Box<dyn FnOnce(&dyn CircuitCatalog) + Send>;

/// How a pipeline suite builds a controller, beyond its circuit and its configuration.
#[derive(Default)]
pub struct ControllerOptions {
    /// Called with the catalog of the circuit before the controller opens the connectors.
    pub setup: Option<CatalogSetup>,

    /// The controller bootstraps the parts of the circuit that changed since the checkpoint
    /// that it resumes from, rather than taking the circuit as unchanged.
    pub keep_program_diff: bool,
}

/// An engine as the pipeline suites see it: the connector fixture, and the circuits that
/// controllers and servers run.
pub trait PipelineTestEngine: ConnectorTestEngine {
    /// Returns the factory of `circuit`, which calls `setup`, if any, with the catalog of the
    /// circuit before the controller opens the connectors. A server starts its pipeline from
    /// such a factory.
    ///
    /// Fails if the engine does not support the relations of `circuit`.
    fn circuit_factory(
        &self,
        circuit: &TestCircuit,
        setup: Option<CatalogSetup>,
    ) -> Result<CircuitFactory, ControllerError>;

    /// Returns the function that opens the storage that a pipeline's configuration names, as
    /// the engine's [`Engine::open_storage`](crate::engine::Engine::open_storage) does.
    fn open_storage(&self) -> OpenStorageFn;

    /// Leaves the storage in the directory `path` as a crash after publishing a checkpoint,
    /// but before recording it as the one to resume from, leaves it: with the state of the
    /// older checkpoint `checkpoint` as the state to resume from.
    fn rewind_resume_state(&self, path: &Path, checkpoint: Uuid);

    /// Returns whether no pipeline of the engine holds the storage in the directory `path`
    /// any more, so that a new pipeline can open it.
    fn storage_released(&self, path: &Path) -> bool;

    /// Returns a controller that runs `circuit` with `config`, and reports errors to
    /// `error_callback`.
    ///
    /// Fails if the engine does not support the relations of `circuit`, or if the controller
    /// does not start.
    fn controller(
        &self,
        circuit: &TestCircuit,
        config: &PipelineConfig,
        error_callback: ControllerErrorCallback,
    ) -> Result<Controller, ControllerError> {
        self.controller_with_options(
            circuit,
            config,
            ControllerOptions::default(),
            error_callback,
        )
    }

    /// Like [`Self::controller`], but builds the controller as `options` say.
    fn controller_with_options(
        &self,
        circuit: &TestCircuit,
        config: &PipelineConfig,
        options: ControllerOptions,
        error_callback: ControllerErrorCallback,
    ) -> Result<Controller, ControllerError> {
        let factory = self.circuit_factory(circuit, options.setup)?;
        let open_storage = self.open_storage();
        if options.keep_program_diff {
            Controller::with_test_config_keep_program_diff(
                factory,
                &open_storage,
                config,
                error_callback,
            )
        } else {
            Controller::with_test_config(factory, &open_storage, config, error_callback)
        }
    }
}

/// A circuit that a pipeline suite runs.
#[derive(Clone, Debug)]
pub enum TestCircuit {
    /// Tables `test_input1` to `test_input{n}` and views `test_output1` to `test_output{n}`,
    /// all with the columns of `relation`. Each view holds the rows of its table.
    Identity {
        /// The columns of the tables and the views. The engine serves the relation by its
        /// name.
        relation: Relation,

        /// One entry per view: its persistent id, if any.
        persistent_output_ids: Vec<Option<String>>,

        /// Properties of each table, as the SQL compiler records its `WITH` clause.
        input_properties: Vec<(String, String)>,

        /// The tables and views are materialized.
        materialized: bool,

        /// The tables have persistent ids, which a checkpoint needs: `input` for a single
        /// table, `input1` to `input{n}` for several, each with the suffix `_v{table_version}`
        /// if `table_version` is not 0.
        persistent_ids: bool,

        /// The version of the tables' definitions. A later version gives the tables new
        /// persistent ids, as a changed `CREATE TABLE` does, so that a restart rebuilds them.
        table_version: u32,

        /// The connector attribute that the DEFAULT of a column reads, by column, the same in
        /// each table, as in
        /// [`InputOptions::attribute_defaults`](feldera_adapterlib::test::InputOptions::attribute_defaults).
        attribute_defaults: BTreeMap<String, String>,
    },

    /// Tables `test_input1` to `test_input{n}`, each with the columns of `relation` and a
    /// primary key of the columns of `key`, so that an insert of an existing key replaces its
    /// row. Views `test_output1` to `test_output{n}` hold the rows of their tables, and each
    /// view `test_output{i}` has an index `idx{i}` on the key.
    Keyed {
        /// The key columns. The engine serves the index by the name of `key`.
        key: Relation,

        /// The columns of the tables and the views.
        relation: Relation,

        /// One entry per view: its persistent id, if any. The index of a view with id `id`
        /// has the id `{id}.idx`.
        persistent_output_ids: Vec<Option<String>>,

        /// Each index is an alias of its view, as the SQL compiler registers the first index
        /// of a materialized view, rather than a separate stream.
        aliased: bool,
    },

    /// A table `test_input1` with the columns of `relation` and no primary key, a view
    /// `test_output1` that holds its rows, and an index `index` on the view, keyed by the
    /// columns of `key`. The index can hold several rows under one key.
    IndexedView {
        /// The key columns. The engine serves the index by the name of `key`.
        key: Relation,

        /// The columns of the table and the view.
        relation: Relation,

        /// The name of the index.
        index: String,
    },

    /// A table `test_input1` with the columns of `relation` and a primary key of the columns
    /// of `key`, a view `test_output1` of its rows, and the index `index` on the view, keyed
    /// by `key`. The index is an alias of the view, as the SQL compiler registers the first
    /// index of a materialized view: connectors on the view and on the index read one output
    /// stream. The table has the persistent id `input`, and the view the persistent id
    /// `output`.
    AliasedIndex {
        /// The key columns. The engine serves the index by the name of `key`.
        key: Relation,

        /// The columns of the table and the view.
        relation: Relation,

        /// The name of the index.
        index: String,
    },

    /// A table `test_input1` with the columns of `relation`, a view `passthrough` of its
    /// rows, and a view `counts` with the columns of [`count_columns`]: for each key that
    /// [`skewed_key`] maps the `id` of a row of `passthrough` to, the number of such rows. The
    /// table and the views are materialized, and each has a persistent id: `input`,
    /// `passthrough`, and `counts`. Every operator that keeps state has a persistent id as
    /// well, so that the circuit can be checkpointed.
    Aggregate {
        /// The columns of the table and of `passthrough`, which include `id BIGINT NOT NULL`.
        relation: Relation,

        /// The number of keys that [`skewed_key`] maps ids to.
        n_keys: i64,
    },

    /// The table `now`, which the controller's clock connector fills, and a materialized view
    /// `now_ticks` of every timestamp that the table receives, with the persistent id
    /// `now_ticks`.
    Clock,
}

impl TestCircuit {
    /// Returns the relation whose rows the tables of the circuit hold, unless the circuit
    /// has a fixed schema.
    pub fn relation(&self) -> Option<&Relation> {
        match self {
            Self::Identity { relation, .. }
            | Self::Keyed { relation, .. }
            | Self::IndexedView { relation, .. }
            | Self::AliasedIndex { relation, .. }
            | Self::Aggregate { relation, .. } => Some(relation),
            Self::Clock => None,
        }
    }

    /// Returns the key relation of the circuit's index, if it has one.
    pub fn key(&self) -> Option<&Relation> {
        match self {
            Self::Identity { .. } | Self::Aggregate { .. } | Self::Clock => None,
            Self::Keyed { key, .. }
            | Self::IndexedView { key, .. }
            | Self::AliasedIndex { key, .. } => Some(key),
        }
    }

    /// Returns the connector attribute that the DEFAULT of a column of the circuit's tables
    /// reads, by column.
    pub fn attribute_defaults(&self) -> &BTreeMap<String, String> {
        static NONE: BTreeMap<String, String> = BTreeMap::new();
        match self {
            Self::Identity {
                attribute_defaults, ..
            } => attribute_defaults,
            Self::Keyed { .. }
            | Self::IndexedView { .. }
            | Self::AliasedIndex { .. }
            | Self::Aggregate { .. }
            | Self::Clock => &NONE,
        }
    }

    /// Returns the circuit with `defaults` as the attribute defaults of its tables.
    ///
    /// Panics unless the circuit is [`TestCircuit::Identity`], the only one whose tables take
    /// attribute defaults.
    pub fn with_attribute_defaults(mut self, defaults: BTreeMap<String, String>) -> Self {
        match &mut self {
            Self::Identity {
                attribute_defaults, ..
            } => *attribute_defaults = defaults,
            other => panic!("only an identity circuit takes attribute defaults, not {other:?}"),
        }
        self
    }

    /// Returns one materialized table `test_input1` that feeds one materialized view
    /// `test_output1`, both with the columns of `relation`.
    pub fn identity(relation: Relation) -> Self {
        Self::Identity {
            relation,
            persistent_output_ids: vec![None],
            input_properties: Vec::new(),
            materialized: true,
            persistent_ids: true,
            table_version: 0,
            attribute_defaults: BTreeMap::new(),
        }
    }
}

/// Returns the columns of the view `counts` of [`TestCircuit::Aggregate`]: `key BIGINT NOT
/// NULL` and `n BIGINT NOT NULL`.
pub fn count_columns() -> Vec<Field> {
    vec![
        Field::new("key".into(), ColumnType::bigint(false)),
        Field::new("n".into(), ColumnType::bigint(false)),
    ]
}

/// Maps a row id onto one of `n_keys` keys of [`TestCircuit::Aggregate`].
///
/// Up to 64 keys, the keys follow a skewed distribution: 45% of the ids map to the first
/// key, 36% to the second, then a long tail, so that most of the aggregate's input goes
/// through one or two workers. Above 64 keys, the ids spread evenly, so that the aggregate
/// builds a large trace.
pub fn skewed_key(id: i64, n_keys: i64) -> i64 {
    if n_keys > 64 {
        return id.rem_euclid(n_keys);
    }

    // Cumulative percentages of 15 event types, scaled to 10,000.
    const CUMULATIVE: [i64; 15] = [
        4489, 8093, 9001, 9587, 9727, 9845, 9926, 9979, 9988, 9992, 9996, 9998, 9999, 10000, 10000,
    ];
    let bucket = (id * 7919).rem_euclid(10_000);
    let key = CUMULATIVE.iter().position(|c| bucket < *c).unwrap_or(14) as i64;
    key % n_keys
}

/// Returns the rows that the ad-hoc SQL `query` returns on the latest consistent snapshot of
/// the pipeline that `controller` runs.
pub fn query(controller: &Controller, query: &str) -> AnyResult<Vec<RecordBatch>> {
    TOKIO.block_on(async {
        Ok(crate::adhoc::execute_sql(controller, query)
            .await?
            .collect()
            .await?)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::TestCircuit;
    use crate::test::relations;

    /// An identity circuit has no attribute defaults until it is given some, and a circuit
    /// that takes none has none.
    #[test]
    fn an_identity_circuit_takes_attribute_defaults() {
        let circuit = TestCircuit::identity(relations::basic("defaults"));
        assert!(circuit.attribute_defaults().is_empty());
        let defaults = BTreeMap::from([("s".to_string(), "kafka_topic".to_string())]);
        assert_eq!(
            circuit
                .with_attribute_defaults(defaults.clone())
                .attribute_defaults(),
            &defaults
        );
        assert!(TestCircuit::Clock.attribute_defaults().is_empty());
    }

    #[test]
    #[should_panic(expected = "only an identity circuit takes attribute defaults")]
    fn only_an_identity_circuit_takes_attribute_defaults() {
        let _ = TestCircuit::Clock.with_attribute_defaults(BTreeMap::new());
    }
}
