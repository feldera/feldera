//! Compiled record types for the relations of the connector suites.

use std::collections::BTreeMap;

use feldera_macros::{IsNone, OrdRepr};
use feldera_sqllib::{
    ByteArray, Date, F32, F64, FlatVariant, SqlDecimal, SqlString, SqlVariant, Time, Timestamp,
    TimestampTz, Uuid, Variant, cast_to_TimestampN_FVN, cast_to_bN_FVN, cast_to_i32N_FVN,
    cast_to_i64N_FVN, cast_to_sN_FVN, cast_to_u32N_FVN, indexFVN_,
};
use feldera_types::{deserialize_table_record, serialize_table_record};
use size_of::SizeOf;

/// Counts the token trees it is given.
macro_rules! count {
    () => { 0usize };
    ($head:tt $($tail:tt)*) => { 1usize + count!($($tail)*) };
}

/// Declares a record type of a table named `$table`, with one field per column: the field,
/// its type, the column name, and the value of a column that a record omits (`|_| None` makes
/// the column required).
macro_rules! record {
    (
        $(#[$meta:meta])*
        $name:ident[$table:literal] {
            $($field:ident: $type:ty = $column:literal, $missing:expr),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Clone,
            Hash,
            SizeOf,
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            IsNone,
            OrdRepr,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        pub struct $name {
            $(pub $field: $type,)*
        }

        deserialize_table_record!($name[$table, SqlVariant, count!($($field)*)] {
            $(($field, $column, false, $type, $missing)),*
        });

        serialize_table_record!($name[count!($($field)*)]{
            $($field[$column]: $type),*
        });
    };
}

record! {
    /// A row of [`json_parser::relation`](crate::test::suites::json_parser::relation).
    JsonInput["json_input"] {
        b: bool = "b", |_| None,
        i: i32 = "i", |_| None,
        s: Option<String> = "s", |_| Some(None),
    }
}

record! {
    /// A row of [`csv_parser::relation`](crate::test::suites::csv_parser::relation).
    CsvInput["csv_input"] {
        first: String = "first", |_| None,
        second: String = "second", |_| None,
    }
}

record! {
    /// A row of a relation with the columns of [`relations::basic`](crate::test::relations::basic).
    Basic["basic"] {
        id: i64 = "id", |_| None,
        b: bool = "b", |_| None,
        i: Option<i64> = "i", |_| Some(None),
        s: String = "s", |_| None,
    }
}

record! {
    /// A key of a relation with the columns of
    /// [`relations::basic_key`](crate::test::relations::basic_key).
    BasicKey["basic_key"] {
        id: i64 = "id", |_| None,
    }
}

record! {
    /// A row of a relation with the columns of
    /// [`relations::every_type`](crate::test::relations::every_type).
    EveryType["every_type"] {
        bigint: i64 = "bigint", |_| None,
        binary: ByteArray = "binary", |_| None,
        boolean: bool = "boolean", |_| None,
        date: Date = "date", |_| None,
        decimal_10_3: SqlDecimal<10, 3> = "decimal_10_3", |_| None,
        double: F64 = "double", |_| None,
        float: F32 = "float", |_| None,
        int: i32 = "int", |_| None,
        smallint: i16 = "smallint", |_| None,
        string: String = "string", |_| None,
        unused: Option<String> = "unused", |_| Some(None),
        timestamp_ntz: Timestamp = "timestamp_ntz", |_| None,
        timestamp_tz: TimestampTz = "timestamp_tz", |_| None,
        tinyint: i8 = "tinyint", |_| None,
        string_array: Vec<String> = "string_array", |_| None,
        struct1: Basic = "struct1", |_| None,
        struct_array: Vec<Basic> = "struct_array", |_| None,
        string_string_map: BTreeMap<String, String> = "string_string_map", |_| None,
        string_struct_map: BTreeMap<String, Basic> = "string_struct_map", |_| None,
        variant: Variant = "variant", |_| None,
        uuid: Uuid = "uuid", |_| None,
    }
}

/// Returns the connector attribute `name`, as `CONNECTOR_METADATA()[name]` reads it.
fn attribute(metadata: &Option<SqlVariant>, name: &str) -> Option<SqlVariant> {
    indexFVN_(metadata, SqlString::from_ref(name))
}

/// Returns the connector attribute `kafka_topic` as a string, if the record has metadata.
fn kafka_topic(metadata: &Option<SqlVariant>) -> Option<Option<SqlString>> {
    metadata.as_ref().map(|_| {
        cast_to_sN_FVN(attribute(metadata, "kafka_topic"), -1, false)
            .ok()
            .flatten()
    })
}

record! {
    /// A row of [`raw_parser::varchar_relation`](crate::test::suites::raw_parser::varchar_relation).
    RawVarchar["raw_varchar"] {
        data: SqlString = "data", |_| None,
    }
}

record! {
    /// A row of
    /// [`raw_parser::varchar_metadata_relation`](crate::test::suites::raw_parser::varchar_metadata_relation).
    RawVarcharMetadata["raw_varchar_metadata"] {
        data: SqlString = "data", |_| None,
        kafka_topic: Option<SqlString> = "kafka_topic", kafka_topic,
    }
}

record! {
    /// A row of
    /// [`raw_parser::nullable_varchar_relation`](crate::test::suites::raw_parser::nullable_varchar_relation).
    RawNullableVarchar["raw_nullable_varchar"] {
        data: Option<SqlString> = "data", |_| Some(None),
    }
}

record! {
    /// A row of [`raw_parser::varbinary_relation`](crate::test::suites::raw_parser::varbinary_relation).
    RawVarbinary["raw_varbinary"] {
        data: ByteArray = "data", |_| None,
    }
}

record! {
    /// A row of
    /// [`raw_parser::nullable_varbinary_relation`](crate::test::suites::raw_parser::nullable_varbinary_relation).
    RawNullableVarbinary["raw_nullable_varbinary"] {
        data: Option<ByteArray> = "data", |_| Some(None),
    }
}

record! {
    /// A row of
    /// [`raw_parser::nullable_varbinary_metadata_relation`](crate::test::suites::raw_parser::nullable_varbinary_metadata_relation).
    RawNullableVarbinaryMetadata["raw_nullable_varbinary_metadata"] {
        data: Option<ByteArray> = "data", |_| Some(None),
        kafka_topic: Option<SqlString> = "kafka_topic", kafka_topic,
    }
}

record! {
    /// A row of
    /// [`raw_parser::nullable_varbinary_nulls_relation`](crate::test::suites::raw_parser::nullable_varbinary_nulls_relation).
    RawNullableVarbinaryNulls["raw_nullable_varbinary_nulls"] {
        data: Option<ByteArray> = "data", |_| Some(None),
        kafka_topic: Option<SqlString> = "kafka_topic", |_| Some(None),
    }
}

record! {
    /// A row of
    /// [`avro_parser::cdc_field_relation`](crate::test::suites::avro_parser::cdc_field_relation).
    AvroCdcField["avro_cdc_field"] {
        id: i64 = "id", |_| None,
        s: String = "s", |_| None,
    }
}

record! {
    /// A row of
    /// [`avro_parser::binary_relation`](crate::test::suites::avro_parser::binary_relation).
    AvroBinary["avro_binary"] {
        binary32: ByteArray = "binary32", |_| None,
        varbinary: ByteArray = "varbinary", |_| None,
    }
}

record! {
    /// A row of [`avro_parser::uuid_relation`](crate::test::suites::avro_parser::uuid_relation).
    AvroUuid["avro_uuid"] {
        uuid1: Uuid = "uuid1", |_| None,
        uuid2: Uuid = "uuid2", |_| None,
        varchar: String = "varchar", |_| None,
    }
}

record! {
    /// A row of
    /// [`avro_parser::variant_relation`](crate::test::suites::avro_parser::variant_relation),
    /// with the VARIANTs in both of the engine's representations.
    AvroVariant["avro_variant"] {
        v1: Variant = "v1", |_| None,
        v2: FlatVariant = "v2", |_| None,
    }
}

record! {
    /// A row of [`avro_parser::enum_relation`](crate::test::suites::avro_parser::enum_relation).
    AvroEnum["avro_enum"] {
        enum_val: String = "enum_val", |_| None,
    }
}

record! {
    /// A row of
    /// [`avro_parser::metadata_relation`](crate::test::suites::avro_parser::metadata_relation).
    AvroMetadata["avro_metadata"] {
        id: i64 = "id", |_| None,
        schema_id: Option<u32> = "schema_id", |metadata: &Option<SqlVariant>| {
            metadata.as_ref().map(|_| {
                cast_to_u32N_FVN(attribute(metadata, "avro_schema_id"))
                    .ok()
                    .flatten()
            })
        },
    }
}

record! {
    /// A row of
    /// [`avro_parser::temporal_relation`](crate::test::suites::avro_parser::temporal_relation).
    AvroTemporal["avro_temporal"] {
        id: i64 = "id", |_| None,
        ts_millis: Timestamp = "ts_millis", |_| None,
        ts_micros: Timestamp = "ts_micros", |_| None,
        ts_nanos: Timestamp = "ts_nanos", |_| None,
        ts_zoned: TimestampTz = "ts_zoned", |_| None,
        ts_connect: Timestamp = "ts_connect", |_| None,
        ts_opt: Option<Timestamp> = "ts_opt", |_| Some(None),
        t_millis: Time = "t_millis", |_| None,
        t_micros: Time = "t_micros", |_| None,
        t_nanos: Time = "t_nanos", |_| None,
        t_zoned: Time = "t_zoned", |_| None,
        t_connect: Time = "t_connect", |_| None,
        dt: Date = "dt", |_| None,
    }
}

record! {
    /// A row of
    /// [`avro_parser::timestamp_targets_relation`](crate::test::suites::avro_parser::timestamp_targets_relation).
    AvroTimestampTargets["avro_timestamp_targets"] {
        z_ts: Timestamp = "z_ts", |_| None,
        z_tstz: TimestampTz = "z_tstz", |_| None,
        n_ts: Timestamp = "n_ts", |_| None,
        n_tstz: TimestampTz = "n_tstz", |_| None,
    }
}

record! {
    /// A row of
    /// [`avro_parser::decimal_relation`](crate::test::suites::avro_parser::decimal_relation).
    AvroDecimal["avro_decimal"] {
        id: i64 = "id", |_| None,
        amount: SqlDecimal<10, 3> = "amount", |_| None,
        amount_opt: Option<SqlDecimal<10, 3>> = "amount_opt", |_| Some(None),
    }
}

record! {
    /// A row of
    /// [`avro_parser::integers_relation`](crate::test::suites::avro_parser::integers_relation).
    AvroIntegers["avro_integers"] {
        uint: u32 = "uint", |_| None,
        ulong: u64 = "ulong", |_| None,
        int: i32 = "int", |_| None,
        long: i64 = "long", |_| None,
    }
}

record! {
    /// A row of [`parquet::variant_relation`](crate::test::suites::parquet::variant_relation).
    ParquetVariant["parquet_variant"] {
        id: i64 = "id", |_| None,
        v: Option<Variant> = "v", |_| Some(None),
        json_v: Option<Variant> = "json_v", |_| Some(None),
    }
}

record! {
    /// A row of [`datagen::real_relation`](crate::test::suites::datagen::real_relation).
    DatagenReal["datagen_real"] {
        double: F64 = "double", |_| None,
        dec: SqlDecimal<28, 0> = "dec", |_| None,
        dec_scale: SqlDecimal<10, 10> = "dec_scale", |_| None,
        dec_w_scale: SqlDecimal<2, 1> = "dec_w_scale", |_| None,
    }
}

record! {
    /// A row of [`datagen::bytes_relation`](crate::test::suites::datagen::bytes_relation).
    DatagenBytes["datagen_bytes"] {
        bs: ByteArray = "bs", |_| None,
    }
}

record! {
    /// A row of [`datagen::time_relation`](crate::test::suites::datagen::time_relation).
    DatagenTime["datagen_time"] {
        ts: Timestamp = "ts", |_| None,
        dt: Date = "dt", |_| None,
        t: Time = "t", |_| None,
    }
}

record! {
    /// A row of [`file_input::relation`](crate::test::suites::file_input::relation),
    /// [`url_input::relation`](crate::test::suites::url_input::relation), and the NATS input
    /// suite's relation.
    StringBoolInt["string_bool_int"] {
        s: String = "s", |_| None,
        b: bool = "b", |_| None,
        i: i64 = "i", |_| None,
    }
}

record! {
    /// A row of the DynamoDB suite's view.
    DynamoRow["dynamo_row"] {
        id: i32 = "id", |_| None,
        sort: String = "sort", |_| None,
        b: bool = "b", |_| None,
        i: Option<i64> = "i", |_| Some(None),
        s: String = "s", |_| None,
    }
}

record! {
    /// The key of [`DynamoRow`].
    DynamoKey["dynamo_key"] {
        id: i32 = "id", |_| None,
        sort: String = "sort", |_| None,
    }
}

record! {
    /// A row of the DynamoDB suite's view with a column of every type that the connector
    /// writes.
    DynamoAllTypes["dynamo_all_types"] {
        id: i32 = "id", |_| None,
        boolean_: bool = "boolean_", |_| None,
        tinyint_: i8 = "tinyint_", |_| None,
        smallint_: i16 = "smallint_", |_| None,
        int_: i32 = "int_", |_| None,
        bigint_: i64 = "bigint_", |_| None,
        decimal_: SqlDecimal<38, 10> = "decimal_", |_| None,
        float_: F32 = "float_", |_| None,
        double_: F64 = "double_", |_| None,
        varchar_: SqlString = "varchar_", |_| None,
        time_: Time = "time_", |_| None,
        date_: Date = "date_", |_| None,
        timestamp_: Timestamp = "timestamp_", |_| None,
        variant_: Variant = "variant_", |_| None,
        uuid_: Uuid = "uuid_", |_| None,
        varbinary_: ByteArray = "varbinary_", |_| None,
        struct_: Basic = "struct_", |_| None,
        string_array_: Vec<SqlString> = "string_array_", |_| None,
        struct_array_: Vec<Basic> = "struct_array_", |_| None,
        map_: BTreeMap<SqlString, Basic> = "map_", |_| None,
        nullable_: Option<i64> = "nullable_", |_| Some(None),
    }
}

record! {
    /// The key of [`DynamoAllTypes`].
    DynamoAllTypesKey["dynamo_all_types_key"] {
        id: i32 = "id", |_| None,
    }
}

record! {
    /// A row of the view of the Delta Lake merge suite.
    DeltaMergeRow["delta_merge"] {
        id: i64 = "id", |_| None,
        payload: String = "payload", |_| None,
    }
}

record! {
    /// The key of [`DeltaMergeRow`].
    DeltaMergeKey["k"] {
        id: i64 = "id", |_| None,
    }
}

/// A table whose column names are quoted uppercase SQL identifiers, as a table
/// imported from a system that folds identifiers to upper case carries.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct UpperCaseTestStruct {
    pub id: i64,
    pub payload: String,
    pub region: String,
}

serialize_table_record!(UpperCaseTestStruct[3]{
    id["ID"]: i64,
    payload["PAYLOAD"]: String,
    region["REGION"]: String
});

// Deserialized case-sensitively: the connector must deliver each column under
// the name the Delta table gives it, not a case-folded one.
deserialize_table_record!(UpperCaseTestStruct["UpperCaseTestStruct", SqlVariant, 3] {
    (id, "ID", true, i64, |_| None),
    (payload, "PAYLOAD", true, String, |_| None),
    (region, "REGION", true, String, |_| None)
});

/// The same table declared in SQL without quotes, so the two sides spell the
/// columns differently.
///
/// An unquoted SQL identifier folds to lower case, and the compiler then emits a
/// deserializer that matches column names ignoring case: the `false` below is
/// the column's `isQuoted()` flag, and the names are the folded ones.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct UnquotedSqlTestStruct {
    pub id: i64,
    pub payload: String,
    pub region: String,
}

serialize_table_record!(UnquotedSqlTestStruct[3]{
    id["id"]: i64,
    payload["payload"]: String,
    region["region"]: String
});

deserialize_table_record!(UnquotedSqlTestStruct["UnquotedSqlTestStruct", SqlVariant, 3] {
    (id, "id", false, i64, |_| None),
    (payload, "payload", false, String, |_| None),
    (region, "region", false, String, |_| None)
});

// ---- Delta `variant` columns ----------------------------------------------

/// Table read by the VARIANT tests: one column stored as a Delta `variant`,
/// one stored as a Delta `string` holding JSON text.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct VariantTestStruct {
    pub id: i64,
    pub v: Option<Variant>,
    pub json_v: Option<Variant>,
}

serialize_table_record!(VariantTestStruct[3]{
    id["id"]: i64,
    v["v"]: Option<Variant>,
    json_v["json_v"]: Option<Variant>
});

deserialize_table_record!(VariantTestStruct["VariantTestStruct", SqlVariant, 3] {
    (id, "id", false, i64, |_| None),
    (v, "v", false, Option<Variant>, |_| Some(None)),
    (json_v, "json_v", false, Option<Variant>, |_| Some(None))
});

// ---- Uniform-over-Iceberg tables (columnMapping.mode = 'id') --------------

/// Nested column of the Uniform test table. Its child is column-mapped too, so
/// reading it needs field-id matching inside the struct.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct UniformNested {
    pub merchant: Option<String>,
    pub status: Option<String>,
}

serialize_table_record!(UniformNested[2]{
    merchant["merchant"]: Option<String>,
    status["status"]: Option<String>
});

deserialize_table_record!(UniformNested["UniformNested", SqlVariant, 2] {
    (merchant, "merchant", false, Option<String>, |_| Some(None)),
    (status, "status", false, Option<String>, |_| Some(None))
});

/// SQL relation the Uniform tests read. [`UniformCdcRow`] writes two more
/// columns, so the CDC configuration can name columns the SQL schema does not.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct UniformTestStruct {
    pub id: Option<i64>,
    pub label: Option<String>,
    pub after: Option<UniformNested>,
    pub history: Option<Vec<UniformNested>>,
    pub tags: Option<BTreeMap<String, UniformNested>>,
}

serialize_table_record!(UniformTestStruct[5]{
    id["id"]: Option<i64>,
    label["label"]: Option<String>,
    after["after"]: Option<UniformNested>,
    history["history"]: Option<Vec<UniformNested>>,
    tags["tags"]: Option<BTreeMap<String, UniformNested>>
});

deserialize_table_record!(UniformTestStruct["UniformTestStruct", SqlVariant, 5] {
    (id, "id", false, Option<i64>, |_| Some(None)),
    (label, "label", false, Option<String>, |_| Some(None)),
    (after, "after", false, Option<UniformNested>, |_| Some(None)),
    (history, "history", false, Option<Vec<UniformNested>>, |_| Some(None)),
    (tags, "tags", false, Option<BTreeMap<String, UniformNested>>, |_| Some(None))
});

/// [`UniformTestStruct`] with the `op` and `ts` columns the CDC configuration
/// names but the SQL table never declares.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
    OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct UniformCdcRow {
    pub id: Option<i64>,
    pub label: Option<String>,
    pub after: Option<UniformNested>,
    pub history: Option<Vec<UniformNested>>,
    pub tags: Option<BTreeMap<String, UniformNested>>,
    pub op: Option<String>,
    pub ts: Option<i64>,
}

serialize_table_record!(UniformCdcRow[7]{
    id["id"]: Option<i64>,
    label["label"]: Option<String>,
    after["after"]: Option<UniformNested>,
    history["history"]: Option<Vec<UniformNested>>,
    tags["tags"]: Option<BTreeMap<String, UniformNested>>,
    op["op"]: Option<String>,
    ts["ts"]: Option<i64>
});

/// A row of [`cdc_all_types_relation`](crate::test::suites::postgres_cdc::cdc_all_types_relation),
/// with a column for each Postgres type that the CDC connector reads.
#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
    feldera_macros::OrdRepr,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct CdcAllTypesStruct {
    pub id: i32,
    pub col_text: Option<String>,
    pub col_integer: Option<i32>,
    pub col_bigint: Option<i64>,
    pub col_boolean: Option<bool>,
    pub col_real: Option<feldera_sqllib::F32>,
    pub col_double: Option<feldera_sqllib::F64>,
    pub col_date: Option<String>,
    pub col_time: Option<String>,
    pub col_timetz: Option<String>,
    pub col_timetz_array: Option<Vec<Option<String>>>,
    pub col_timestamp: Option<String>,
    pub col_timestamptz: Option<String>,
    pub col_uuid: Option<String>,
    pub col_jsonb: Option<feldera_sqllib::Variant>,
    pub col_bytea: Option<ByteArray>,
    pub col_numeric: Option<String>,
    pub col_smallint: Option<i16>,
    pub col_int_array: Option<Vec<Option<i32>>>,
}

feldera_types::deserialize_table_record!(CdcAllTypesStruct["CdcAllTypesStruct", SqlVariant, 19] {
    (id, "id", false, i32, |_| None),
    (col_text, "col_text", true, Option<String>, |_| Some(None)),
    (col_integer, "col_integer", true, Option<i32>, |_| Some(None)),
    (col_bigint, "col_bigint", true, Option<i64>, |_| Some(None)),
    (col_boolean, "col_boolean", true, Option<bool>, |_| Some(None)),
    (col_real, "col_real", true, Option<feldera_sqllib::F32>, |_| Some(None)),
    (col_double, "col_double", true, Option<feldera_sqllib::F64>, |_| Some(None)),
    (col_date, "col_date", true, Option<String>, |_| Some(None)),
    (col_time, "col_time", true, Option<String>, |_| Some(None)),
    (col_timetz, "col_timetz", true, Option<String>, |_| Some(None)),
    (col_timetz_array, "col_timetz_array", true, Option<Vec<Option<String>>>, |_| Some(None)),
    (col_timestamp, "col_timestamp", true, Option<String>, |_| Some(None)),
    (col_timestamptz, "col_timestamptz", true, Option<String>, |_| Some(None)),
    (col_uuid, "col_uuid", true, Option<String>, |_| Some(None)),
    (col_jsonb, "col_jsonb", true, Option<feldera_sqllib::Variant>, |_| Some(None)),
    (col_bytea, "col_bytea", true, Option<ByteArray>, |_| Some(None)),
    (col_numeric, "col_numeric", true, Option<String>, |_| Some(None)),
    (col_smallint, "col_smallint", true, Option<i16>, |_| Some(None)),
    (col_int_array, "col_int_array", true, Option<Vec<Option<i32>>>, |_| Some(None))
});

feldera_types::serialize_table_record!(CdcAllTypesStruct[19]{
    id["id"]: i32,
    col_text["col_text"]: Option<String>,
    col_integer["col_integer"]: Option<i32>,
    col_bigint["col_bigint"]: Option<i64>,
    col_boolean["col_boolean"]: Option<bool>,
    col_real["col_real"]: Option<feldera_sqllib::F32>,
    col_double["col_double"]: Option<feldera_sqllib::F64>,
    col_date["col_date"]: Option<String>,
    col_time["col_time"]: Option<String>,
    col_timetz["col_timetz"]: Option<String>,
    col_timetz_array["col_timetz_array"]: Option<Vec<Option<String>>>,
    col_timestamp["col_timestamp"]: Option<String>,
    col_timestamptz["col_timestamptz"]: Option<String>,
    col_uuid["col_uuid"]: Option<String>,
    col_jsonb["col_jsonb"]: Option<feldera_sqllib::Variant>,
    col_bytea["col_bytea"]: Option<ByteArray>,
    col_numeric["col_numeric"]: Option<String>,
    col_smallint["col_smallint"]: Option<i16>,
    col_int_array["col_int_array"]: Option<Vec<Option<i32>>>
});

record! {
    /// A row of
    /// [`cdc_temporal_relation`](crate::test::suites::postgres_cdc::cdc_temporal_relation): the
    /// temporal types that have Postgres values Feldera cannot hold.
    CdcTemporal["postgres_cdc_temporal"] {
        id: i32 = "id", |_| None,
        d: Option<Date> = "d", |_| Some(None),
        t: Option<Time> = "t", |_| Some(None),
    }
}

record! {
    /// A row of [`postgres::relation`](crate::test::suites::postgres::relation).
    PostgresAll["postgres"] {
        boolean_: bool = "boolean_", |_| None,
        tinyint_: i8 = "tinyint_", |_| None,
        smallint_: i16 = "smallint_", |_| None,
        int_: i32 = "int_", |_| None,
        bigint_: i64 = "bigint_", |_| None,
        decimal_: SqlDecimal<10, 3> = "decimal_", |_| None,
        float_: F32 = "float_", |_| None,
        double_: F64 = "double_", |_| None,
        varchar_: SqlString = "varchar_", |_| None,
        time_: Time = "time_", |_| None,
        date_: Date = "date_", |_| None,
        timestamp_: Timestamp = "timestamp_", |_| None,
        variant_: Variant = "variant_", |_| None,
        uuid_: Uuid = "uuid_", |_| None,
        varbinary_: ByteArray = "varbinary_", |_| None,
        struct_: Basic = "struct_", |_| None,
        string_array_: Vec<SqlString> = "string_array_", |_| None,
        struct_array_: Vec<Basic> = "struct_array_", |_| None,
        map_: BTreeMap<SqlString, Basic> = "map_", |_| None,
    }
}

record! {
    /// The key of [`PostgresAll`].
    PostgresAllKey["postgres_key"] {
        bigint_: i64 = "bigint_", |_| None,
    }
}

record! {
    /// A row of the view of the Postgres output suite.
    PostgresRow["postgres_view"] {
        id: i32 = "id", |_| None,
        b: bool = "b", |_| None,
        i: Option<i64> = "i", |_| Some(None),
        s: String = "s", |_| None,
    }
}

record! {
    /// The key of [`PostgresRow`].
    PostgresKey["postgres_idx"] {
        id: i32 = "id", |_| None,
    }
}

record! {
    /// A row of the view `counts` of
    /// [`TestCircuit::Aggregate`](crate::test::pipeline::TestCircuit::Aggregate).
    Count["counts"] {
        key: i64 = "key", |_| None,
        n: i64 = "n", |_| None,
    }
}

record! {
    /// A row of [`s3_input::relation`](crate::test::suites::s3_input::relation).
    SingleBigint["single_bigint"] {
        i: i64 = "i", |_| None,
    }
}

record! {
    /// A row of [`soft_delete::raw_relation`](crate::test::suites::soft_delete::raw_relation).
    RawSoftDelete["soft_delete_raw"] {
        s: String = "s", |_| None,
        is_delete: Option<bool> = "is_delete", |metadata: &Option<SqlVariant>| {
            Some(cast_to_bN_FVN(attribute(metadata, "is_delete")).ok().flatten())
        },
    }
}

record! {
    /// A row of the table `now` and the view `now_ticks` of the clock circuit.
    Clock["now"] {
        now: Timestamp = "now", |_| None,
    }
}

impl From<dbsp::utils::Tup1<Timestamp>> for Clock {
    fn from(row: dbsp::utils::Tup1<Timestamp>) -> Self {
        Self { now: row.0 }
    }
}

impl From<Clock> for dbsp::utils::Tup1<Timestamp> {
    fn from(row: Clock) -> Self {
        dbsp::utils::Tup1::new(row.now)
    }
}

record! {
    /// A row of [`kafka::raw_metadata_relation`](crate::test::suites::kafka::raw_metadata_relation).
    KafkaRawMetadata["kafka_raw_metadata"] {
        data: SqlString = "data", |_| None,
        kafka_headers: SqlVariant = "kafka_headers", |metadata: &Option<SqlVariant>| {
            attribute(metadata, "kafka_headers")
        },
        kafka_topic: SqlString = "kafka_topic", |metadata: &Option<SqlVariant>| {
            cast_to_sN_FVN(attribute(metadata, "kafka_topic"), -1, false).ok().flatten()
        },
        kafka_timestamp: Timestamp = "kafka_timestamp", |metadata: &Option<SqlVariant>| {
            cast_to_TimestampN_FVN(attribute(metadata, "kafka_timestamp")).ok().flatten()
        },
        kafka_partition: i32 = "kafka_partition", |metadata: &Option<SqlVariant>| {
            cast_to_i32N_FVN(attribute(metadata, "kafka_partition")).ok().flatten()
        },
        kafka_offset: i64 = "kafka_offset", |metadata: &Option<SqlVariant>| {
            cast_to_i64N_FVN(attribute(metadata, "kafka_offset")).ok().flatten()
        },
    }
}
