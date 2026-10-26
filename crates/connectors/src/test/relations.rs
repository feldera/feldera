//! Column sets that several suites declare their relations with.

use std::collections::BTreeMap;

use feldera_adapterlib::test::InputOptions;
use feldera_types::program_schema::{ColumnType, Field, Relation};

/// Returns a relation named `name` with four columns: `id BIGINT`, `b BOOLEAN`,
/// `i BIGINT NULL`, and `s VARCHAR`.
pub fn basic(name: &str) -> Relation {
    Relation::new(name.into(), basic_columns(), false, BTreeMap::new())
}

/// Returns a relation named `name` with the key column of [`basic`]: `id BIGINT`.
pub fn basic_key(name: &str) -> Relation {
    Relation::new(
        name.into(),
        vec![Field::new("id".into(), ColumnType::bigint(false))],
        false,
        BTreeMap::new(),
    )
}

/// Returns the columns of [`basic`].
pub fn basic_columns() -> Vec<Field> {
    vec![
        Field::new("id".into(), ColumnType::bigint(false)),
        Field::new("b".into(), ColumnType::boolean(false)),
        Field::new("i".into(), ColumnType::bigint(true)),
        Field::new("s".into(), ColumnType::varchar(false)),
    ]
}

/// Returns a relation named `name` with a column of each type that most formats hold:
/// integers, floats, a decimal, strings, binary, dates and timestamps, arrays, structs,
/// maps, VARIANT, and UUID. Its column `unused` is marked unused.
pub fn every_type(name: &str) -> Relation {
    let basic_struct = || ColumnType::structure(false, &basic_columns());
    Relation::new(
        name.into(),
        vec![
            Field::new("bigint".into(), ColumnType::bigint(false)),
            Field::new("binary".into(), ColumnType::varbinary(false)),
            Field::new("boolean".into(), ColumnType::boolean(false)),
            Field::new("date".into(), ColumnType::date(false)),
            Field::new("decimal_10_3".into(), ColumnType::decimal(10, 3, false)),
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("float".into(), ColumnType::real(false)),
            Field::new("int".into(), ColumnType::int(false)),
            Field::new("smallint".into(), ColumnType::smallint(false)),
            Field::new("string".into(), ColumnType::varchar(false)),
            Field::new("unused".into(), ColumnType::varchar(true)).with_unused(true),
            Field::new("timestamp_ntz".into(), ColumnType::timestamp(false)),
            Field::new("timestamp_tz".into(), ColumnType::timestamp_tz(false)),
            Field::new("tinyint".into(), ColumnType::tinyint(false)),
            Field::new(
                "string_array".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new("struct1".into(), basic_struct()),
            Field::new(
                "struct_array".into(),
                ColumnType::array(false, basic_struct()),
            ),
            Field::new(
                "string_string_map".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::varchar(false),
                ),
            ),
            Field::new(
                "string_struct_map".into(),
                ColumnType::map(false, ColumnType::varchar(false), basic_struct()),
            ),
            Field::new("variant".into(), ColumnType::variant(false)),
            Field::new("uuid".into(), ColumnType::uuid(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns a relation named `name` with columns of assorted types: `id BIGINT`,
/// `"nAmE" VARCHAR NULL` (a case-sensitive name), `b BOOLEAN`, `ts TIMESTAMP`, `dt DATE`,
/// `es ROW(a BOOLEAN) NULL`, `m MAP<VARCHAR, BIGINT> NULL`, and `dec DECIMAL(10, 3)`.
pub fn assorted(name: &str) -> Relation {
    Relation::new(
        name.into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("\"nAmE\"".into(), ColumnType::varchar(true)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("ts".into(), ColumnType::timestamp(false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new(
                "es".into(),
                ColumnType::structure(true, &[Field::new("a".into(), ColumnType::boolean(false))]),
            ),
            Field::new(
                "m".into(),
                ColumnType::map(true, ColumnType::varchar(false), ColumnType::bigint(false)),
            ),
            Field::new("dec".into(), ColumnType::decimal(10, 3, false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns a relation named `name` for a table with soft deletes: `id BIGINT`,
/// `s VARCHAR`, and `is_delete BOOLEAN NULL`, whose DEFAULT reads the connector attribute
/// `is_delete`, as [`soft_delete_options`] declares.
pub fn soft_delete(name: &str) -> Relation {
    Relation::new(
        name.into(),
        vec![
            Field::new("id".into(), ColumnType::bigint(false)),
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("is_delete".into(), ColumnType::boolean(true)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns the input options of a [`soft_delete`] table: soft deletes, and the DEFAULT of
/// `is_delete` reads the attribute of the same name.
pub fn soft_delete_options() -> InputOptions {
    InputOptions {
        soft_delete: true,
        attribute_defaults: BTreeMap::from([("is_delete".to_string(), "is_delete".to_string())]),
    }
}
