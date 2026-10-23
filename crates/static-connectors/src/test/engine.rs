//! The static engine as the shared connector suites see it.
//!
//! The static engine moves records through types that the SQL compiler generates, so the
//! fixture serves a fixed set of relations, each with a compiled test type, and converts
//! between that type and the relation's published Arrow form.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow, bail};
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use dbsp::circuit::CircuitConfig;
use dbsp::storage::dirlock::LockedDirectory;
use dbsp::utils::{Tup1, Tup2};
use dbsp::{DBData, DBSPHandle, OrdIndexedZSet, OrdZSet, Runtime};
use feldera_adapterlib::catalog::{InputCollectionHandle, SerBatchReader};
use feldera_adapterlib::soft_delete::SoftDeleteHandle;
use feldera_adapterlib::test::{
    Capability, ConnectorTestEngine, InputOptions, TestCollection, WEIGHT_COLUMN,
};
use feldera_sqllib::{SqlVariant, Timestamp};
use feldera_types::constants::STATE_FILE;
use feldera_types::program_schema::{
    ColumnType, Field, PropertyValue, Relation, SourcePosition, SqlIdentifier,
};
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
use uuid::Uuid;

use crate::engine::{static_circuit_factory, static_open_storage};
use crate::seroutput::SerBatchImpl;
use crate::test::data::TestStructMetadata;
use crate::test::pipeline::{
    CatalogSetup, PipelineTestEngine, TestCircuit, count_columns, skewed_key,
};
use crate::test::suites::{
    arrow_ipc, controller, csv_parser, datagen, file_input, json_encoder, json_parser, parquet,
    raw_parser, s3_input, soft_delete, url_input,
};
use crate::test::{KeyStruct, MockDeZSet, MockUpdate, TestStruct, TestStruct2};
use crate::test::{published_schema, relations};
use crate::{Catalog, CircuitCatalog, ControllerError};
use dbsp_adapters::engine::OpenStorageFn;
use dbsp_adapters::format::parquet::default_arrow_serde_config;

pub(super) mod records;

/// A compiled record type that the fixture serves a relation with.
trait CompiledRecord:
    DBData
    + Default
    + SerializeWithContext<SqlSerdeConfig>
    + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
    + Sync
{
}

impl<T> CompiledRecord for T where
    T: DBData
        + Default
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Sync
{
}

/// The static engine's fixture.
pub struct StaticTestEngine {
    relations: BTreeMap<SqlIdentifier, ServedRelation>,

    /// The indexes that the fixture serves, by the name of the key relation.
    indexes: BTreeMap<SqlIdentifier, ServedIndex>,
}

/// A relation that the fixture serves, and the compiled type that serves it.
struct ServedRelation {
    relation: Relation,

    /// The attribute defaults that the type compiles in: the attribute of each column, by
    /// column.
    attribute_defaults: BTreeMap<String, String>,

    record_type: Arc<dyn RecordType>,
}

/// An index that the fixture serves: its key, the relation that it indexes, and the
/// compiled types that serve them.
struct ServedIndex {
    key: Relation,
    relation: Relation,
    index_type: Arc<dyn IndexType>,
}

impl StaticTestEngine {
    pub fn new() -> Self {
        let mut engine = Self {
            relations: BTreeMap::new(),
            indexes: BTreeMap::new(),
        };
        engine.serve::<TestStruct>(TestStruct::relation_schema(), BTreeMap::new());
        engine.serve_index::<KeyStruct, TestStruct>(
            KeyStruct::relation_schema(),
            TestStruct::relation_schema(),
            |row| KeyStruct { id: row.id },
        );
        engine.serve::<TestStruct2>(TestStruct2::relation_schema(), BTreeMap::new());
        engine.serve::<records::Basic>(controller::relation(), BTreeMap::new());
        engine.serve_index::<records::BasicKey, records::Basic>(
            controller::key_relation(),
            controller::relation(),
            |row| records::BasicKey { id: row.id },
        );
        engine.serve::<records::CsvInput>(csv_parser::relation(), BTreeMap::new());
        engine.serve::<records::Basic>(csv_parser::basic_relation(), BTreeMap::new());
        engine.serve::<TestStruct2>(datagen::assorted_relation(), BTreeMap::new());
        engine.serve::<records::DatagenReal>(datagen::real_relation(), BTreeMap::new());
        engine.serve::<records::DatagenBytes>(datagen::bytes_relation(), BTreeMap::new());
        engine.serve::<records::DatagenTime>(datagen::time_relation(), BTreeMap::new());
        engine.serve::<records::Basic>(json_encoder::relation(), BTreeMap::new());
        engine.serve::<records::JsonInput>(json_parser::relation(), BTreeMap::new());
        engine.serve::<records::EveryType>(json_parser::every_type_relation(), BTreeMap::new());
        engine.serve::<TestStructMetadata>(
            json_parser::metadata_relation(),
            json_parser::metadata_options().attribute_defaults,
        );
        engine.serve::<TestStruct2>(parquet::relation(), BTreeMap::new());
        engine.serve::<records::ParquetVariant>(parquet::variant_relation(), BTreeMap::new());
        engine.serve::<TestStruct2>(arrow_ipc::assorted_relation(), BTreeMap::new());
        engine.serve::<records::EveryType>(arrow_ipc::every_type_relation(), BTreeMap::new());
        engine.serve::<crate::test::TestStructSoftDelete>(
            soft_delete::relation(),
            crate::test::relations::soft_delete_options().attribute_defaults,
        );
        engine.serve::<records::RawSoftDelete>(
            soft_delete::raw_relation(),
            crate::test::relations::soft_delete_options().attribute_defaults,
        );
        engine.serve_index::<records::BasicKey, records::Basic>(
            soft_delete::key_relation(),
            soft_delete::keyed_relation(),
            |row| records::BasicKey { id: row.id },
        );
        #[cfg(feature = "with-kafka")]
        {
            use crate::test::suites::kafka;
            engine.serve::<records::Basic>(kafka::relation(), BTreeMap::new());
            engine.serve::<records::KafkaRawMetadata>(
                kafka::raw_metadata_relation(),
                kafka::raw_metadata_options().attribute_defaults,
            );
        }
        #[cfg(feature = "with-nats")]
        {
            use crate::test::suites::{nats_input, nats_pipeline};
            engine.serve::<records::StringBoolInt>(nats_input::relation(), BTreeMap::new());
            engine.serve::<records::Basic>(nats_pipeline::relation(), BTreeMap::new());
        }
        #[cfg(feature = "with-postgres-cdc")]
        {
            use crate::test::suites::postgres_cdc;
            use crate::test::suites::postgres_cdc_destination;
            engine.serve::<records::Basic>(postgres_cdc::cdc_relation(), BTreeMap::new());
            engine.serve::<records::Basic>(postgres_cdc_destination::relation(), BTreeMap::new());
            engine.serve::<records::CdcAllTypesStruct>(
                postgres_cdc::cdc_all_types_relation(),
                BTreeMap::new(),
            );
            engine.serve::<records::CdcTemporal>(
                postgres_cdc::cdc_temporal_relation(),
                BTreeMap::new(),
            );
        }
        {
            use crate::test::suites::postgres;
            engine.serve_index::<records::PostgresAllKey, records::PostgresAll>(
                postgres::key_relation(),
                postgres::relation(),
                |row| records::PostgresAllKey {
                    bigint_: row.bigint_,
                },
            );
            engine.serve::<records::Basic>(postgres::basic_relation(), BTreeMap::new());
            engine.serve_index::<records::BasicKey, records::Basic>(
                postgres::basic_key_relation(),
                postgres::basic_relation(),
                |row| records::BasicKey { id: row.id },
            );
        }
        {
            use crate::test::suites::postgres_output;
            engine.serve_index::<records::PostgresKey, records::PostgresRow>(
                postgres_output::key_relation(),
                postgres_output::value_relation(),
                |row| records::PostgresKey { id: row.id },
            );
        }
        #[cfg(feature = "with-deltalake")]
        {
            use crate::test::suites::delta_input;
            use crate::test::suites::delta_table;
            engine.serve::<records::Basic>(delta_input::stream_relation(), BTreeMap::new());
            use crate::test::data::DeltaTestKey;
            engine.serve::<records::EveryType>(delta_table::relation(), BTreeMap::new());
            engine.serve::<records::EveryType>(delta_table::lateness_relation(), BTreeMap::new());
            engine.serve_index::<DeltaTestKey, records::EveryType>(
                delta_table::key_relation(),
                delta_table::relation(),
                |row| DeltaTestKey { bigint: row.bigint },
            );
            engine.serve::<records::Basic>(delta_table::basic_relation(), BTreeMap::new());
            engine.serve::<records::Basic>(delta_table::basic_unused_i_relation(), BTreeMap::new());
            engine.serve_index::<records::BasicKey, records::Basic>(
                delta_table::basic_key_relation(),
                delta_table::basic_relation(),
                |row| records::BasicKey { id: row.id },
            );
            engine.serve::<crate::test::TestStructSoftDelete>(
                delta_table::soft_delete_relation(),
                crate::test::relations::soft_delete_options().attribute_defaults,
            );
            engine.serve::<records::UpperCaseTestStruct>(
                delta_table::upper_case_relation(),
                BTreeMap::new(),
            );
            engine.serve::<records::UpperCaseTestStruct>(
                delta_table::upper_case_lateness_relation(),
                BTreeMap::new(),
            );
            engine.serve::<records::UnquotedSqlTestStruct>(
                delta_table::unquoted_relation(),
                BTreeMap::new(),
            );
            engine.serve::<records::VariantTestStruct>(
                delta_table::variant_relation(),
                BTreeMap::new(),
            );
            engine.serve::<records::UniformTestStruct>(
                delta_table::uniform_relation(),
                BTreeMap::new(),
            );
            #[cfg(any(feature = "delta-s3-test", feature = "delta-unity-test"))]
            engine.serve::<crate::test::DatabricksPeople>(
                delta_table::people_relation(),
                BTreeMap::new(),
            );
        }
        #[cfg(feature = "with-deltalake")]
        {
            use crate::test::suites::delta_merge;
            engine.serve_index::<records::DeltaMergeKey, records::DeltaMergeRow>(
                delta_merge::key_relation(),
                delta_merge::value_relation(),
                |row| records::DeltaMergeKey { id: row.id },
            );
        }
        #[cfg(feature = "with-deltalake")]
        {
            use crate::test::data::{
                DeltaTestKey, DeltaTestKeyBinary, DeltaTestKeyDate, DeltaTestKeyDecimal,
                DeltaTestKeyDouble, DeltaTestKeyFloat, DeltaTestKeyInt, DeltaTestKeySmallInt,
                DeltaTestKeyString, DeltaTestKeyStruct, DeltaTestKeyTimestamp, DeltaTestKeyTinyInt,
                DeltaTestKeyUnused, DeltaTestKeyUuid, DeltaTestStruct,
            };
            use crate::test::suites::delta_output;
            engine.serve::<DeltaTestStruct>(delta_output::value_relation(), BTreeMap::new());
            engine.serve_index::<DeltaTestKey, DeltaTestStruct>(
                delta_output::key_relation_on("bigint"),
                delta_output::value_relation(),
                |row| DeltaTestKey { bigint: row.bigint },
            );
            engine.serve_index::<DeltaTestKeyInt, DeltaTestStruct>(
                delta_output::key_relation_on("int"),
                delta_output::value_relation(),
                |row| DeltaTestKeyInt { int: row.int },
            );
            engine.serve_index::<DeltaTestKeyString, DeltaTestStruct>(
                delta_output::key_relation_on("string"),
                delta_output::value_relation(),
                |row| DeltaTestKeyString {
                    string: row.string.clone(),
                },
            );
            engine.serve_index::<DeltaTestKeyBinary, DeltaTestStruct>(
                delta_output::key_relation_on("binary"),
                delta_output::value_relation(),
                |row| DeltaTestKeyBinary {
                    binary: row.binary.clone(),
                },
            );
            engine.serve_index::<DeltaTestKeyTimestamp, DeltaTestStruct>(
                delta_output::key_relation_on("timestamp_ntz"),
                delta_output::value_relation(),
                |row| DeltaTestKeyTimestamp {
                    timestamp_ntz: row.timestamp_ntz,
                },
            );
            engine.serve_index::<DeltaTestKeyDecimal, DeltaTestStruct>(
                delta_output::key_relation_on("decimal_10_3"),
                delta_output::value_relation(),
                |row| DeltaTestKeyDecimal {
                    decimal_10_3: row.decimal_10_3,
                },
            );
            engine.serve_index::<DeltaTestKeyUuid, DeltaTestStruct>(
                delta_output::key_relation_on("uuid"),
                delta_output::value_relation(),
                |row| DeltaTestKeyUuid {
                    uuid: row.uuid.clone(),
                },
            );
            engine.serve_index::<DeltaTestKeyDate, DeltaTestStruct>(
                delta_output::key_relation_on("date"),
                delta_output::value_relation(),
                |row| DeltaTestKeyDate { date: row.date },
            );
            engine.serve_index::<DeltaTestKeyStruct, DeltaTestStruct>(
                delta_output::key_relation_on("struct1"),
                delta_output::value_relation(),
                |row| DeltaTestKeyStruct {
                    struct1: row.struct1.clone(),
                },
            );
            engine.serve_index::<DeltaTestKeyDouble, DeltaTestStruct>(
                delta_output::key_relation_on("double"),
                delta_output::value_relation(),
                |row| DeltaTestKeyDouble { double: row.double },
            );
            engine.serve_index::<DeltaTestKeyFloat, DeltaTestStruct>(
                delta_output::key_relation_on("float"),
                delta_output::value_relation(),
                |row| DeltaTestKeyFloat { float: row.float },
            );
            engine.serve_index::<DeltaTestKeyTinyInt, DeltaTestStruct>(
                delta_output::key_relation_on("tinyint"),
                delta_output::value_relation(),
                |row| DeltaTestKeyTinyInt {
                    tinyint: row.tinyint,
                },
            );
            engine.serve_index::<DeltaTestKeySmallInt, DeltaTestStruct>(
                delta_output::key_relation_on("smallint"),
                delta_output::value_relation(),
                |row| DeltaTestKeySmallInt {
                    smallint: row.smallint,
                },
            );
            engine.serve_index::<DeltaTestKeyUnused, DeltaTestStruct>(
                delta_output::key_relation_on("unused"),
                delta_output::value_relation(),
                |row| DeltaTestKeyUnused {
                    unused: row.unused.clone(),
                },
            );
        }
        #[cfg(feature = "with-dynamodb")]
        {
            use crate::test::suites::dynamodb;
            engine.serve_index::<records::DynamoKey, records::DynamoRow>(
                dynamodb::key_relation(),
                dynamodb::value_relation(),
                |row| records::DynamoKey {
                    id: row.id,
                    sort: row.sort.clone(),
                },
            );
            engine.serve_index::<records::DynamoAllTypesKey, records::DynamoAllTypes>(
                dynamodb::all_types_key_relation(),
                dynamodb::all_types_value_relation(),
                |row| records::DynamoAllTypesKey { id: row.id },
            );
        }
        #[cfg(feature = "with-redis")]
        {
            use crate::test::suites::redis;
            engine.serve::<records::EveryType>(redis::relation(), BTreeMap::new());
            engine.serve::<records::Basic>(redis::basic_relation(), BTreeMap::new());
        }
        #[cfg(feature = "with-pubsub")]
        engine.serve::<records::Basic>(crate::test::suites::pubsub::relation(), BTreeMap::new());
        #[cfg(feature = "with-iceberg")]
        {
            use crate::test::suites::{iceberg, iceberg_s3tables};
            use crate::test::{IcebergSubsetTestStruct, IcebergTestStruct, S3TablesTestStruct};
            engine.serve::<IcebergTestStruct>(iceberg::relation(), BTreeMap::new());
            engine.serve::<IcebergTestStruct>(iceberg::lateness_relation(), BTreeMap::new());
            engine.serve::<IcebergSubsetTestStruct>(iceberg::subset_relation(), BTreeMap::new());
            engine.serve::<S3TablesTestStruct>(
                iceberg_s3tables::s3tables_relation(),
                BTreeMap::new(),
            );
        }
        engine.serve::<records::StringBoolInt>(file_input::relation(), BTreeMap::new());
        engine.serve::<records::StringBoolInt>(url_input::relation(), BTreeMap::new());
        engine.serve::<records::SingleBigint>(s3_input::relation(), BTreeMap::new());
        engine.serve::<records::RawVarchar>(raw_parser::varchar_relation(), BTreeMap::new());
        engine.serve::<records::RawVarcharMetadata>(
            raw_parser::varchar_metadata_relation(),
            raw_parser::metadata_options().attribute_defaults,
        );
        engine.serve::<records::RawNullableVarchar>(
            raw_parser::nullable_varchar_relation(),
            BTreeMap::new(),
        );
        engine.serve::<records::RawVarbinary>(raw_parser::varbinary_relation(), BTreeMap::new());
        engine.serve::<records::RawNullableVarbinary>(
            raw_parser::nullable_varbinary_relation(),
            BTreeMap::new(),
        );
        engine.serve::<records::RawNullableVarbinaryMetadata>(
            raw_parser::nullable_varbinary_metadata_relation(),
            raw_parser::metadata_options().attribute_defaults,
        );
        engine.serve::<records::RawNullableVarbinaryNulls>(
            raw_parser::nullable_varbinary_nulls_relation(),
            BTreeMap::new(),
        );
        #[cfg(feature = "with-avro")]
        engine.serve_avro();
        engine.serve_index::<records::BasicKey, records::Basic>(
            index_key(),
            indexed_relation(),
            |row| records::BasicKey { id: row.id },
        );
        engine
    }

    /// Serves the relations of the Avro suites.
    #[cfg(feature = "with-avro")]
    fn serve_avro(&mut self) {
        use crate::test::TestStructSoftDelete;
        use crate::test::suites::{avro_encoder, avro_parser};

        self.serve::<TestStruct2>(avro_parser::assorted_relation(), BTreeMap::new());
        self.serve::<TestStructSoftDelete>(
            avro_parser::soft_delete_relation(),
            crate::test::relations::soft_delete_options().attribute_defaults,
        );
        self.serve::<records::BasicKey>(avro_parser::id_relation(), BTreeMap::new());
        self.serve::<records::AvroCdcField>(avro_parser::cdc_field_relation(), BTreeMap::new());
        self.serve::<records::AvroBinary>(avro_parser::binary_relation(), BTreeMap::new());
        self.serve::<records::AvroUuid>(avro_parser::uuid_relation(), BTreeMap::new());
        self.serve::<records::AvroVariant>(avro_parser::variant_relation(), BTreeMap::new());
        self.serve::<records::AvroEnum>(avro_parser::enum_relation(), BTreeMap::new());
        self.serve::<records::AvroMetadata>(
            avro_parser::metadata_relation(),
            avro_parser::metadata_options().attribute_defaults,
        );
        self.serve::<records::AvroTemporal>(avro_parser::temporal_relation(), BTreeMap::new());
        self.serve::<records::AvroTimestampTargets>(
            avro_parser::timestamp_targets_relation(),
            BTreeMap::new(),
        );
        self.serve::<records::AvroDecimal>(avro_parser::decimal_relation(), BTreeMap::new());
        self.serve::<records::AvroIntegers>(avro_parser::integers_relation(), BTreeMap::new());
        self.serve::<records::Basic>(avro_encoder::relation(), BTreeMap::new());
        self.serve::<TestStruct2>(avro_encoder::output_assorted_relation(), BTreeMap::new());
        self.serve::<records::EveryType>(avro_encoder::every_type_relation(), BTreeMap::new());
        self.serve_index::<records::BasicKey, records::Basic>(
            avro_encoder::key_relation(),
            avro_encoder::relation(),
            |row| records::BasicKey { id: row.id },
        );
    }

    /// Serves `relation` with records of type `T`, whose column defaults read the connector
    /// attributes in `attribute_defaults`, by column.
    fn serve<T: CompiledRecord>(
        &mut self,
        relation: Relation,
        attribute_defaults: BTreeMap<String, String>,
    ) {
        let served = ServedRelation {
            relation: relation.clone(),
            attribute_defaults,
            record_type: Arc::new(Records::<T>(PhantomData)),
        };
        self.relations.insert(relation.name, served);
    }

    /// Serves the index `key` on `relation` with keys of type `K` and values of type `V`,
    /// where `key_of` returns the key of a value.
    fn serve_index<K: CompiledRecord, V: CompiledRecord>(
        &mut self,
        key: Relation,
        relation: Relation,
        key_of: fn(&V) -> K,
    ) {
        let served = ServedIndex {
            key: key.clone(),
            relation,
            index_type: Arc::new(Index { key_of }),
        };
        self.indexes.insert(key.name, served);
    }

    /// Returns the types that serve the index `key` on `relation`, if they serve exactly
    /// their columns.
    fn served_index(&self, key: &Relation, relation: &Relation) -> Option<&ServedIndex> {
        self.indexes.get(&key.name).filter(|served| {
            served.key.fields == key.fields
                && served.relation.name == relation.name
                && served.relation.fields == relation.fields
        })
    }

    /// Returns the type that serves `relation`, if one serves exactly its columns.
    fn served(&self, relation: &Relation) -> Option<&ServedRelation> {
        self.relations.get(&relation.name).filter(|served| {
            served.relation.fields == relation.fields
                && served.relation.primary_key == relation.primary_key
        })
    }

    /// Returns `true` if the engine can build an input or output for `relation` with
    /// `options`.
    ///
    /// The static engine must precompile a fixed set of relation types, and only those
    /// relations are available at runtime.
    fn supports(&self, relation: &Relation, options: &InputOptions) -> bool {
        self.served(relation)
            .is_some_and(|served| served.attribute_defaults == options.attribute_defaults)
    }

    /// Builds the circuit of `circuit` with `config`, without a controller, for the tests of
    /// the static engine that read a circuit or its catalog.
    ///
    /// Fails if the fixture does not serve the relations of `circuit`.
    pub fn circuit(
        &self,
        circuit: &TestCircuit,
        config: CircuitConfig,
    ) -> Result<(DBSPHandle, Box<dyn CircuitCatalog>), ControllerError> {
        Ok(self.circuit_builder(circuit)?.build(config, circuit))
    }

    /// Returns the builder of the circuit of `circuit`.
    ///
    /// Fails if the fixture does not serve the relations of `circuit`.
    fn circuit_builder(
        &self,
        circuit: &TestCircuit,
    ) -> Result<Box<dyn CircuitFactory>, ControllerError> {
        let builder: Box<dyn CircuitFactory> = match (circuit.relation(), circuit.key()) {
            (Some(relation), _) if matches!(circuit, TestCircuit::Aggregate { .. }) => {
                if relation.fields != relations::basic_columns() {
                    return Err(ControllerError::not_supported(&format!(
                        "the static engine's fixture builds the aggregate circuit only over the columns of `relations::basic`, not over those of '{}'",
                        relation.name
                    )));
                }
                Box::new(AggregateCircuit)
            }
            (None, _) => Box::new(ClockCircuit),
            (Some(relation), None) => {
                let served = self.served(relation).ok_or_else(|| {
                    ControllerError::not_supported(&format!(
                        "the static engine's fixture serves no relation '{}' with these columns",
                        relation.name
                    ))
                })?;
                // The record type compiles its attribute defaults in, so the circuit must
                // declare the same ones.
                if &served.attribute_defaults != circuit.attribute_defaults() {
                    return Err(ControllerError::not_supported(&format!(
                        "the static engine's fixture serves relation '{}' with the attribute defaults {:?}, not {:?}",
                        relation.name,
                        served.attribute_defaults,
                        circuit.attribute_defaults()
                    )));
                }
                Box::new(RecordCircuit(served.record_type.clone()))
            }
            (Some(relation), Some(key)) => {
                let served = self.served_index(key, relation).ok_or_else(|| {
                    ControllerError::not_supported(&format!(
                        "the static engine's fixture serves no index '{}' on '{}' with these columns",
                        key.name, relation.name
                    ))
                })?;
                Box::new(IndexCircuit(served.index_type.clone()))
            }
        };
        Ok(builder)
    }
}

impl Default for StaticTestEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorTestEngine for StaticTestEngine {
    fn name(&self) -> &'static str {
        "static"
    }

    /// dbsp's floats store `-0.0` as `0.0`, so no key of the static engine is `-0.0`.
    fn has(&self, capability: Capability) -> bool {
        capability != Capability::NegativeZeroKeys
    }

    fn input_collection(
        &self,
        relation: &Relation,
        options: &InputOptions,
    ) -> AnyResult<Box<dyn TestCollection>> {
        if !self.supports(relation, options) {
            bail!(
                "the static engine's fixture serves no relation '{}' with these columns and {options:?}",
                relation.name
            );
        }
        let served = self.served(relation).unwrap();
        Ok(served.record_type.input_collection(relation, options))
    }

    fn output_batch(
        &self,
        relation: &Relation,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>> {
        let served = self.served(relation).ok_or_else(|| {
            anyhow!(
                "the static engine's fixture serves no relation '{}' with these columns",
                relation.name
            )
        })?;
        served.record_type.output_batch(rows, weights)
    }

    fn indexed_output_batch(
        &self,
        key: &Relation,
        relation: &Relation,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>> {
        let served = self.served_index(key, relation).ok_or_else(|| {
            anyhow!(
                "the static engine's fixture serves no index '{}' on '{}' with these columns",
                key.name,
                relation.name
            )
        })?;
        served.index_type.output_batch(rows, weights)
    }
}

impl PipelineTestEngine for StaticTestEngine {
    fn circuit_factory(
        &self,
        circuit: &TestCircuit,
        setup: Option<CatalogSetup>,
    ) -> Result<dbsp_adapters::engine::CircuitFactory, ControllerError> {
        let factory = self.circuit_builder(circuit)?;
        let circuit = circuit.clone();
        Ok(static_circuit_factory(Box::new(move |config| {
            let (handle, catalog) = factory.build(config, &circuit);
            if let Some(setup) = setup {
                setup(catalog.as_ref());
            }
            Ok((handle, catalog))
        })))
    }

    fn open_storage(&self) -> OpenStorageFn {
        static_open_storage()
    }

    /// The root `state.json` names the state to resume from, and each checkpoint keeps its
    /// own copy of its state.
    fn rewind_resume_state(&self, path: &Path, checkpoint: Uuid) {
        std::fs::copy(
            path.join(checkpoint.to_string()).join(STATE_FILE),
            path.join(STATE_FILE),
        )
        .unwrap();
    }

    /// dbsp locks a pipeline's storage with a lock file, so the storage is free once the
    /// lock can be taken. Dropping the lock releases it at once.
    fn storage_released(&self, path: &Path) -> bool {
        LockedDirectory::new(path).is_ok()
    }
}

/// Builds the dbsp circuit of a [`TestCircuit`].
trait CircuitFactory: Send {
    fn build(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>);
}

/// Builds a circuit without an index over the records of one type.
struct RecordCircuit(Arc<dyn RecordType>);

impl CircuitFactory for RecordCircuit {
    fn build(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        self.0.circuit(config, circuit)
    }
}

/// Builds the clock circuit.
struct ClockCircuit;

impl CircuitFactory for ClockCircuit {
    fn build(
        &self,
        config: CircuitConfig,
        _circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
            let mut catalog = Catalog::new();
            let relation = |name: &str| {
                serde_json::to_string(&Relation::new(
                    name.into(),
                    vec![Field::new("now".into(), ColumnType::timestamp(false))],
                    false,
                    BTreeMap::new(),
                ))
                .unwrap()
            };
            let (now, handle) = circuit.add_input_zset::<Tup1<Timestamp>>();
            now.set_persistent_id(Some("now"));
            catalog.register_input_zset::<_, records::Clock>(now.clone(), handle, &relation("now"));
            catalog.register_materialized_output_zset_persistent::<_, records::Clock>(
                Some("now_ticks"),
                now,
                &relation("now_ticks"),
            );
            Ok(catalog)
        })
        .unwrap();
        (circuit, Box::new(catalog))
    }
}

/// Builds [`TestCircuit::Aggregate`] over the rows of [`records::Basic`].
struct AggregateCircuit;

impl CircuitFactory for AggregateCircuit {
    fn build(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        let TestCircuit::Aggregate { relation, n_keys } = circuit else {
            unreachable!("the aggregate factory builds only the aggregate circuit")
        };
        let fields = relation.fields.clone();
        let n_keys = *n_keys;
        let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
            let mut catalog = Catalog::new();
            let schema = |name: &str, fields: Vec<Field>| {
                serde_json::to_string(&Relation::new(name.into(), fields, false, BTreeMap::new()))
                    .unwrap()
            };
            let (input, hinput) = circuit.add_input_zset::<records::Basic>();
            input.set_persistent_mir_id("input");
            catalog.register_materialized_input_zset::<OrdZSet<records::Basic>, records::Basic>(
                input.clone(),
                hinput,
                &schema("test_input1", fields.clone()),
            );
            let view = input.map(|row| row.clone());
            catalog.register_materialized_output_zset_persistent::<
                OrdZSet<records::Basic>,
                records::Basic,
            >(Some("passthrough"), view.clone(), &schema("passthrough", fields));
            let counts = view
                .map_index(move |row| (skewed_key(row.id, n_keys), ()))
                .weighted_count()
                .map(|(key, n)| records::Count { key: *key, n: *n });
            catalog.register_materialized_output_zset_persistent::<
                OrdZSet<records::Count>,
                records::Count,
            >(Some("counts"), counts, &schema("counts", count_columns()));
            Ok(catalog)
        })
        .unwrap();
        (circuit, Box::new(catalog))
    }
}

/// Builds a circuit with an index of one key type over the records of one type.
struct IndexCircuit(Arc<dyn IndexType>);

impl CircuitFactory for IndexCircuit {
    fn build(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        self.0.circuit(config, circuit)
    }
}

/// What the fixture does with the records of one compiled type.
trait RecordType: Send + Sync {
    fn input_collection(
        &self,
        relation: &Relation,
        options: &InputOptions,
    ) -> Box<dyn TestCollection>;

    fn output_batch(
        &self,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>>;

    fn circuit(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>);
}

/// The records of type `T`.
struct Records<T>(PhantomData<fn() -> T>);

impl<T: CompiledRecord> RecordType for Records<T> {
    fn input_collection(
        &self,
        relation: &Relation,
        options: &InputOptions,
    ) -> Box<dyn TestCollection> {
        Box::new(TypedCollection::<T> {
            relation: relation.clone(),
            handle: MockDeZSet::new(),
            soft_delete: options.soft_delete,
        })
    }

    fn output_batch(
        &self,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>> {
        let tuples = batch_to_records::<T>(rows, weights)?
            .into_iter()
            .zip(weights)
            .map(|(record, weight)| Tup2(record, *weight))
            .collect();
        Ok(Arc::new(SerBatchImpl::<_, T, ()>::new(OrdZSet::from_keys(
            (),
            tuples,
        ))))
    }

    fn circuit(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        match circuit {
            TestCircuit::Identity {
                relation,
                persistent_output_ids,
                input_properties,
                materialized,
                persistent_ids,
                table_version,
                // The record type compiles them in, and `circuit_factory` checks them.
                attribute_defaults: _,
            } => {
                let input_properties: Vec<(&str, &str)> = input_properties
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                let persistent_output_ids: Vec<Option<&str>> =
                    persistent_output_ids.iter().map(Option::as_deref).collect();
                identity_circuit::<T>(
                    config,
                    &relation.fields,
                    &input_properties,
                    &persistent_output_ids,
                    *persistent_ids,
                    *materialized,
                    *table_version,
                )
            }
            TestCircuit::Keyed { .. }
            | TestCircuit::IndexedView { .. }
            | TestCircuit::AliasedIndex { .. }
            | TestCircuit::Aggregate { .. }
            | TestCircuit::Clock => {
                unreachable!("a circuit with an index or a fixed schema has its own factory")
            }
        }
    }
}

/// What the fixture does with the records of one index.
trait IndexType: Send + Sync {
    fn output_batch(
        &self,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>>;

    fn circuit(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>);
}

/// An index with keys of type `K` and values of type `V`.
struct Index<K, V> {
    key_of: fn(&V) -> K,
}

impl<K: CompiledRecord, V: CompiledRecord> IndexType for Index<K, V> {
    fn output_batch(
        &self,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> AnyResult<Arc<dyn SerBatchReader>> {
        let tuples = batch_to_records::<V>(rows, weights)?
            .into_iter()
            .zip(weights)
            .map(|(value, weight)| Tup2(Tup2((self.key_of)(&value), value), *weight))
            .collect();
        Ok(Arc::new(SerBatchImpl::<_, K, V>::new(
            OrdIndexedZSet::from_tuples((), tuples),
        )))
    }

    fn circuit(
        &self,
        config: CircuitConfig,
        circuit: &TestCircuit,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        match circuit {
            TestCircuit::Keyed {
                key,
                relation,
                persistent_output_ids,
                aliased,
            } => {
                let key_fields: Vec<SqlIdentifier> =
                    key.fields.iter().map(|field| field.name.clone()).collect();
                let persistent_output_ids: Vec<Option<&str>> =
                    persistent_output_ids.iter().map(Option::as_deref).collect();
                keyed_circuit::<V, K, _>(
                    config,
                    &relation.fields,
                    &key_fields,
                    self.key_of,
                    &persistent_output_ids,
                    *aliased,
                )
            }
            TestCircuit::IndexedView {
                key,
                relation,
                index,
            } => indexed_view_circuit::<K, V>(config, key, relation, index, self.key_of),
            TestCircuit::AliasedIndex {
                key,
                relation,
                index,
            } => aliased_index_circuit::<K, V>(config, key, relation, index, self.key_of),
            TestCircuit::Identity { .. } | TestCircuit::Aggregate { .. } | TestCircuit::Clock => {
                unreachable!("a circuit without an index has its own factory")
            }
        }
    }
}

/// Returns the circuit of [`TestCircuit::Identity`] over records of type `T`: tables
/// `test_input1` to `test_input{n}` with the columns `schema` and the properties
/// `input_properties`, each feeding a view `test_output{i}`, one per entry of
/// `persistent_output_ids`. `persistent_ids` says whether the tables get persistent ids, which a
/// checkpoint needs, and `materialized` whether the tables and views are materialized. A later
/// `table_version` gives the tables new persistent ids, as a changed table does.
fn identity_circuit<T: CompiledRecord>(
    config: CircuitConfig,
    schema: &[Field],
    input_properties: &[(&str, &str)],
    persistent_output_ids: &[Option<&str>],
    persistent_ids: bool,
    materialized: bool,
    table_version: u32,
) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
    let zero_position = SourcePosition {
        start_line_number: 0,
        start_column: 0,
        end_line_number: 0,
        end_column: 0,
    };
    let input_properties: BTreeMap<String, PropertyValue> = input_properties
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                PropertyValue {
                    value: value.to_string(),
                    key_position: zero_position,
                    value_position: zero_position,
                },
            )
        })
        .collect();

    let schema = schema.to_vec();
    let persistent_output_ids = persistent_output_ids
        .iter()
        .map(|id| id.map(|s| s.to_string()))
        .collect::<Vec<_>>();

    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();
        let n = persistent_output_ids.len();

        for (persistent_output_id, i) in persistent_output_ids.iter().zip(1..) {
            let (input, hinput) = circuit.add_input_zset::<T>();
            if persistent_ids {
                let id = if n > 1 {
                    format!("input{i}")
                } else {
                    "input".to_string()
                };
                let id = if table_version == 0 {
                    id
                } else {
                    format!("{id}_v{table_version}")
                };
                input.set_persistent_mir_id(&id);
            }

            let input_schema = serde_json::to_string(&Relation::new(
                format!("test_input{i}").into(),
                schema.clone(),
                false,
                input_properties.clone(),
            ))
            .unwrap();

            let output_schema = serde_json::to_string(&Relation::new(
                format!("test_output{i}").into(),
                schema.clone(),
                false,
                BTreeMap::new(),
            ))
            .unwrap();

            if materialized {
                catalog.register_materialized_input_zset(input.clone(), hinput, &input_schema);
            } else {
                catalog.register_input_zset(input.clone(), hinput, &input_schema);
            }

            let persistent_output_id = persistent_output_id.as_ref().map(|s| s.as_str());
            if materialized {
                catalog.register_materialized_output_zset_persistent(
                    persistent_output_id,
                    input,
                    &output_schema,
                );
            } else {
                catalog.register_output_zset_persistent(
                    persistent_output_id,
                    input,
                    &output_schema,
                );
            }
        }
        Ok(catalog)
    })
    .unwrap();
    (circuit, Box::new(catalog))
}

/// Returns the circuit of [`TestCircuit::Keyed`] over records of type `T` with keys of type `K`:
/// tables `test_input1` to `test_input{n}` with the columns `schema` and the primary key
/// `key_fields`, whose key `key_func` computes, each feeding a view `test_output{i}`, one per
/// entry of `persistent_output_ids`, with an index `idx{i}` that is an alias of the view if
/// `index_is_alias`.
fn keyed_circuit<T: CompiledRecord, K: CompiledRecord, KF>(
    config: CircuitConfig,
    schema: &[Field],
    key_fields: &[SqlIdentifier],
    key_func: KF,
    persistent_output_ids: &[Option<&str>],
    index_is_alias: bool,
) -> (DBSPHandle, Box<dyn CircuitCatalog>)
where
    KF: Fn(&T) -> K + Clone + Send + Sync + 'static,
{
    let schema = schema.to_vec();

    let persistent_output_ids = persistent_output_ids
        .iter()
        .map(|id| id.map(|s| s.to_string()))
        .collect::<Vec<_>>();

    let key_fields = key_fields.to_vec();

    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();
        let n = persistent_output_ids.len();

        for (persistent_output_id, i) in persistent_output_ids.iter().zip(1..) {
            let (input, hinput) = circuit
                .add_input_map_persistent::<K, T, T, _>(Some(&format!("input{i}")), |val, upd| {
                    *val = upd.clone()
                });
            if n > 1 {
                input.set_persistent_mir_id(&format!("input{i}"));
            } else {
                input.set_persistent_mir_id("input");
            }

            let input_schema = serde_json::to_string(
                &Relation::new(
                    format!("test_input{i}").into(),
                    schema.clone(),
                    false,
                    BTreeMap::new(),
                )
                .with_primary_key(&key_fields),
            )
            .unwrap();

            let output_schema = serde_json::to_string(&Relation::new(
                format!("test_output{i}").into(),
                schema.clone(),
                false,
                BTreeMap::new(),
            ))
            .unwrap();

            catalog.register_materialized_input_map(
                input.clone(),
                hinput,
                key_func.clone(),
                key_func.clone(),
                &input_schema,
            );

            catalog.register_materialized_output_map_persistent(
                persistent_output_id.as_ref().map(|s| s.as_str()),
                input.clone(),
                if index_is_alias {
                    Some(SqlIdentifier::from(&format!("idx{i}")))
                } else {
                    None
                },
                &output_schema,
                &key_fields.iter().map(|id| id.name()).collect::<Vec<_>>(),
            );

            let persistent_index_id = persistent_output_id.as_ref().map(|id| format!("{id}.idx"));

            if !index_is_alias {
                catalog
                    .register_index_persistent::<K, K, T, T>(
                        persistent_index_id.as_deref(),
                        input.clone(),
                        &SqlIdentifier::from(&format!("idx{i}")),
                        &SqlIdentifier::from(&format!("test_output{i}")),
                        &key_fields.iter().map(|id| id.name()).collect::<Vec<_>>(),
                    )
                    .expect("failed to register index");
            }
        }
        Ok(catalog)
    })
    .unwrap();
    (circuit, Box::new(catalog))
}

/// Returns the circuit of [`TestCircuit::IndexedView`]: a table `test_input1` of `relation`
/// without a primary key, a view `test_output1` of its rows, and the index `index` on the
/// view, keyed by `key_of`.
fn indexed_view_circuit<K: CompiledRecord, V: CompiledRecord>(
    config: CircuitConfig,
    key: &Relation,
    relation: &Relation,
    index: &str,
    key_of: fn(&V) -> K,
) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
    let fields = relation.fields.clone();
    let key_fields: Vec<String> = key.fields.iter().map(|field| field.name.name()).collect();
    let index = SqlIdentifier::from(index);
    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();
        let (input, hinput) = circuit.add_input_zset::<V>();
        let schema = |name: &str| {
            serde_json::to_string(&Relation::new(
                name.into(),
                fields.clone(),
                false,
                BTreeMap::new(),
            ))
            .unwrap()
        };
        catalog.register_materialized_input_zset(input.clone(), hinput, &schema("test_input1"));
        catalog.register_materialized_output_zset::<_, V>(input.clone(), &schema("test_output1"));
        let indexed = input.map_index(move |value| (key_of(value), value.clone()));
        catalog
            .register_index::<K, K, V, V>(
                indexed,
                &index,
                &SqlIdentifier::from("test_output1"),
                &key_fields,
            )
            .expect("the index registers");
        Ok(catalog)
    })
    .unwrap();
    (circuit, Box::new(catalog))
}

/// Returns the circuit of [`TestCircuit::AliasedIndex`]: a table `test_input1` of `relation`
/// with the primary key `key`, a view `test_output1` of its rows, and the index `index` on the
/// view, which shares the view's output stream.
fn aliased_index_circuit<K: CompiledRecord, V: CompiledRecord>(
    config: CircuitConfig,
    key: &Relation,
    relation: &Relation,
    index: &str,
    key_of: fn(&V) -> K,
) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
    let fields = relation.fields.clone();
    let key_fields: Vec<SqlIdentifier> =
        key.fields.iter().map(|field| field.name.clone()).collect();
    let index = SqlIdentifier::from(index);
    let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
        let mut catalog = Catalog::new();
        let (input, hinput) = circuit
            .add_input_map_persistent::<K, V, V, _>(Some("input"), |value, update| {
                *value = update.clone()
            });
        input.set_persistent_mir_id("input");
        let schema =
            |name: &str| Relation::new(name.into(), fields.clone(), false, BTreeMap::new());
        let input_schema =
            serde_json::to_string(&schema("test_input1").with_primary_key(&key_fields)).unwrap();
        let view_schema = serde_json::to_string(&schema("test_output1")).unwrap();
        catalog.register_materialized_input_map(
            input.clone(),
            hinput,
            key_of,
            key_of,
            &input_schema,
        );
        catalog.register_materialized_output_map_persistent(
            Some("output"),
            input,
            Some(index),
            &view_schema,
            &key_fields.iter().map(|id| id.name()).collect::<Vec<_>>(),
        );
        Ok(catalog)
    })
    .unwrap();
    (circuit, Box::new(catalog))
}

/// Returns `rows` as records of type `T`.
///
/// Fails if the rows do not fit `T`, or if there is not one weight per row.
fn batch_to_records<T: CompiledRecord>(rows: &RecordBatch, weights: &[i64]) -> AnyResult<Vec<T>> {
    let deserializer = serde_arrow::Deserializer::from_record_batch(rows)?;
    let records = Vec::<T>::deserialize_with_context(deserializer, default_arrow_serde_config())?;
    if records.len() != weights.len() {
        bail!("{} rows, but {} weights", records.len(), weights.len());
    }
    Ok(records)
}

/// An input collection that records the updates that parsers flush in a [`MockDeZSet`], from
/// which a test of the static engine reads them as compiled records: of type `T`, and of type
/// `U` for a partial update.
pub struct TypedCollection<T, U = T> {
    relation: Relation,
    handle: MockDeZSet<T, U>,
    soft_delete: bool,
}

impl<T, U> TypedCollection<T, U> {
    /// Returns a collection of `relation` that records its updates in `handle`.
    pub fn new(relation: Relation, handle: MockDeZSet<T, U>) -> Self {
        Self {
            relation,
            handle,
            soft_delete: false,
        }
    }
}

impl<T, U> TestCollection for TypedCollection<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + SerializeWithContext<SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    fn input_handle(&self) -> InputCollectionHandle {
        let handle = self.handle.clone();
        if self.soft_delete {
            InputCollectionHandle::new(
                self.relation.clone(),
                SoftDeleteHandle::new(Box::new(handle)),
                None,
            )
        } else {
            InputCollectionHandle::new(self.relation.clone(), handle, None)
        }
    }

    fn schema(&self) -> SchemaRef {
        published_schema(&self.relation)
    }

    fn batches(&self) -> AnyResult<Vec<RecordBatch>> {
        let flushed = self.handle.state().flushed.clone();
        if flushed.is_empty() {
            return Ok(Vec::new());
        }
        let mut records = Vec::with_capacity(flushed.len());
        let mut weights = Vec::with_capacity(flushed.len());
        for update in flushed {
            let (record, weight) = match update {
                MockUpdate::Insert(record) => (record, 1),
                MockUpdate::Delete(record) => (record, -1),
                MockUpdate::Update(_) => {
                    bail!(
                        "the static engine's fixture has no keyed tables, so it cannot apply an update"
                    )
                }
            };
            records.push(record);
            weights.push(weight);
        }
        let rows = records_to_batch(&self.relation, &records)?;
        Ok(vec![with_weights(&rows, weights)?])
    }

    fn len(&self) -> usize {
        self.handle.state().flushed.len()
    }

    fn reset(&self) {
        self.handle.reset();
    }
}

/// Returns `records` in the published form of `relation`.
fn records_to_batch<T: SerializeWithContext<SqlSerdeConfig>>(
    relation: &Relation,
    records: &[T],
) -> AnyResult<RecordBatch> {
    let schema = published_schema(relation);
    let mut builder = ArrayBuilder::new(SerdeArrowSchema::try_from(schema.fields().as_ref())?)?;
    for record in records {
        builder.push(SerializeWithContextWrapper::new(
            record,
            default_arrow_serde_config(),
        ))?;
    }
    Ok(builder.to_record_batch()?)
}

/// Returns `rows` followed by a column that holds `weights`.
fn with_weights(rows: &RecordBatch, weights: Vec<i64>) -> AnyResult<RecordBatch> {
    let mut fields = rows.schema().fields().to_vec();
    fields.push(Arc::new(ArrowField::new(
        WEIGHT_COLUMN,
        DataType::Int64,
        false,
    )));
    let mut columns = rows.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(weights)));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// The key of the index that the fixture's own tests read.
fn index_key() -> Relation {
    relations::basic_key("fixture_key")
}

/// The relation that the index of the fixture's own tests indexes.
fn indexed_relation() -> Relation {
    relations::basic("fixture_indexed")
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use feldera_adapterlib::catalog::RecordFormat;
    use feldera_adapterlib::format::{InputBuffer, Parser};
    use feldera_adapterlib::test::{
        ConnectorTestEngine, InputOptions, TestCollection, Updates, ZSet,
    };
    use feldera_types::config::FormatConfig;
    use feldera_types::format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat};
    use feldera_types::program_schema::{Relation, SqlIdentifier};

    use super::{StaticTestEngine, index_key, indexed_relation, published_schema};
    use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
    use crate::test::relations;
    use crate::test::suites::soft_delete;
    use crate::test::{TestStruct, parser};

    /// `TestStruct` rows in its published form.
    fn rows(ids: &[i64], bs: &[bool], is: &[Option<i64>], ss: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            published_schema(&TestStruct::relation_schema()),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(BooleanArray::from(bs.to_vec())),
                Arc::new(Int64Array::from(is.to_vec())),
                Arc::new(StringArray::from(ss.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Parses `json`, in the insert/delete format, into `collection`, and flushes it.
    fn parse(collection: &dyn TestCollection, json: &str) {
        let config = JsonParserConfig {
            update_format: JsonUpdateFormat::InsertDelete,
            json_flavor: JsonFlavor::Default,
            array: false,
            lines: JsonLines::Multiple,
        };
        let mut parser = parser(
            collection,
            &FormatConfig {
                name: Cow::from("json"),
                config: serde_json::to_value(config).unwrap(),
            },
        );
        let (buffer, errors) = parser.parse(json.as_bytes(), None);
        assert_eq!(errors, []);
        buffer.unwrap().flush().unwrap();
    }

    /// What a parser flushes into the fixture's collection reads back as rows in arrival
    /// order and as their Z-set; an output batch reads back through a cursor.
    #[test]
    fn rows_round_trip_through_the_fixture() {
        let engine = StaticTestEngine::new();
        let relation = TestStruct::relation_schema();
        let collection = engine
            .input_collection(&relation, &InputOptions::default())
            .unwrap();
        parse(
            collection.as_ref(),
            r#"{"insert": {"id": 1, "b": true, "i": null, "s": "one"}}
{"insert": {"id": 2, "b": false, "i": 20, "s": "two"}}
{"delete": {"id": 1, "b": true, "i": null, "s": "one"}}
"#,
        );

        let batches = collection.batches().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            ZSet::from_weighted_batch(&batches[0]).unwrap(),
            ZSet::from_batch(
                &rows(
                    &[1, 2, 1],
                    &[true, false, true],
                    &[None, Some(20), None],
                    &["one", "two", "one"]
                ),
                &[1, 1, -1]
            )
            .unwrap()
        );
        let row = rows(&[2], &[false], &[Some(20)], &["two"]);
        assert_eq!(
            collection.zset().unwrap(),
            ZSet::from_batch(&row, &[1]).unwrap()
        );
        collection.reset();
        assert!(collection.zset().unwrap().is_empty());

        let reader = engine.output_batch(&relation, &row, &[3]).unwrap();
        let mut cursor = reader
            .cursor(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let mut json = Vec::new();
        cursor.serialize_key(&mut json).unwrap();
        assert_eq!(
            String::from_utf8(json).unwrap(),
            r#"{"id":2,"b":false,"i":20,"s":"two"}"#
        );
        assert_eq!(cursor.weight(), 3);
        cursor.step_key().unwrap();
        assert!(!cursor.key_valid());
    }

    /// An indexed output batch holds each row, with its weight, under the key that its key
    /// columns hold, in key order.
    #[test]
    fn an_indexed_batch_holds_each_row_under_its_key() {
        let engine = StaticTestEngine::new();
        let relation = indexed_relation();
        let rows = Updates::from_json(
            published_schema(&relation),
            &[
                (2, r#"{"id": 2, "b": true, "s": "two"}"#),
                (1, r#"{"id": 1, "b": false, "i": 10, "s": "one"}"#),
                (-1, r#"{"id": 2, "b": false, "s": "old two"}"#),
            ],
        )
        .unwrap();
        let batch = engine
            .indexed_output_batch(&index_key(), &relation, rows.rows(), rows.weights())
            .unwrap();
        let mut cursor = batch
            .cursor(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let mut entries = Vec::new();
        while cursor.key_valid() {
            let mut key = Vec::new();
            cursor.serialize_key(&mut key).unwrap();
            while cursor.val_valid() {
                let mut value = Vec::new();
                cursor.serialize_val(&mut value).unwrap();
                entries.push((
                    String::from_utf8(key.clone()).unwrap(),
                    String::from_utf8(value).unwrap(),
                    cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key().unwrap();
        }
        assert_eq!(
            entries,
            [
                (
                    r#"{"id":1}"#.to_string(),
                    r#"{"id":1,"b":false,"i":10,"s":"one"}"#.to_string(),
                    1
                ),
                (
                    r#"{"id":2}"#.to_string(),
                    r#"{"id":2,"b":false,"i":null,"s":"old two"}"#.to_string(),
                    -1
                ),
                (
                    r#"{"id":2}"#.to_string(),
                    r#"{"id":2,"b":true,"i":null,"s":"two"}"#.to_string(),
                    2
                ),
            ]
        );
        assert!(
            engine
                .indexed_output_batch(
                    &relations::basic_key("unknown"),
                    &relation,
                    rows.rows(),
                    rows.weights()
                )
                .is_err()
        );
    }

    /// With soft deletes, a deleted record arrives as an insert.
    #[test]
    fn a_soft_delete_collection_takes_a_delete_as_an_insert() {
        let engine = StaticTestEngine::new();
        let options = InputOptions {
            soft_delete: true,
            attribute_defaults: BTreeMap::new(),
        };
        let collection = engine
            .input_collection(&TestStruct::relation_schema(), &options)
            .unwrap();
        parse(
            collection.as_ref(),
            r#"{"insert": {"id": 1, "b": true, "i": null, "s": "one"}}
{"delete": {"id": 1, "b": true, "i": null, "s": "one"}}
"#,
        );
        assert_eq!(
            collection.zset().unwrap(),
            ZSet::from_batch(&rows(&[1], &[true], &[None], &["one"]), &[2]).unwrap()
        );
    }

    /// The fixture has no keyed tables, so it cannot read back a collection that holds an
    /// update record.
    #[test]
    fn an_update_record_is_refused() {
        let engine = StaticTestEngine::new();
        let collection = engine
            .input_collection(&TestStruct::relation_schema(), &InputOptions::default())
            .unwrap();
        parse(
            collection.as_ref(),
            r#"{"update": {"id": 1, "b": true, "i": null, "s": "one"}}
"#,
        );
        assert!(collection.batches().is_err());
        assert!(collection.zset().is_err());
    }

    /// The fixture refuses a relation that no compiled type serves, and options that the
    /// type does not compile in.
    #[test]
    fn unserved_relations_and_options_are_refused() {
        let engine = StaticTestEngine::new();
        let relation = TestStruct::relation_schema();
        let mut renamed = relation.clone();
        renamed.name = SqlIdentifier::from("Unknown");
        let mut narrower: Relation = relation.clone();
        narrower.fields.pop();
        let defaults = InputOptions {
            soft_delete: false,
            attribute_defaults: BTreeMap::from([("s".to_string(), "kafka_topic".to_string())]),
        };

        assert!(engine.supports(&relation, &InputOptions::default()));
        assert!(!engine.supports(&renamed, &InputOptions::default()));
        assert!(!engine.supports(&narrower, &InputOptions::default()));
        assert!(!engine.supports(&relation, &defaults));
        assert!(engine.input_collection(&relation, &defaults).is_err());
        assert!(
            engine
                .output_batch(&renamed, &rows(&[1], &[true], &[None], &["one"]), &[1])
                .is_err()
        );
    }

    /// A circuit declares the attribute defaults that the record type of its relation
    /// compiles in, and the fixture refuses a circuit that declares others.
    #[test]
    fn a_circuit_declares_the_attribute_defaults_of_its_type() {
        let engine = StaticTestEngine::new();
        let defaults = relations::soft_delete_options().attribute_defaults;
        let circuit = TestCircuit::identity(soft_delete::relation());
        assert!(
            engine
                .circuit_factory(
                    &circuit.clone().with_attribute_defaults(defaults.clone()),
                    None
                )
                .is_ok()
        );
        let error = engine.circuit_factory(&circuit, None).err().unwrap();
        assert!(error.to_string().contains("attribute defaults"), "{error}");
        let plain = TestCircuit::identity(TestStruct::relation_schema());
        assert!(engine.circuit_factory(&plain, None).is_ok());
        assert!(
            engine
                .circuit_factory(&plain.with_attribute_defaults(defaults), None)
                .is_err()
        );
    }
}
