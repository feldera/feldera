//! Key validation and encoding for merge mode.
//!
//! Merge mode locates the row to supersede by comparing a key Feldera holds against the
//! same value decoded back from parquet. Both sides go through [`arrow_row::RowConverter`],
//! which turns a value of any supported type into bytes whose comparison matches the
//! logical one, so the probe reduces to a byte comparison and the lookup chunk can be a
//! sorted byte buffer.
//!
//! A type is usable as a key only when equality survives the round trip through the
//! connector's encoding and the target column's physical type. That is a per-type
//! property, which is why [`validate_key_types`] is an allowlist rather than a check for
//! nesting.

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow::row::{RowConverter, Rows, SortField};
use anyhow::{Result as AnyResult, anyhow, bail};
use feldera_types::program_schema::{ColumnType, Relation, SqlType};

/// Reject key types whose round trip through parquet does not preserve Feldera's notion
/// of equality.
///
/// `ROW` is allowed when every leaf is an allowed scalar: it is a composite key in
/// disguise, Delta writes statistics per leaf path, and the row encoding handles nested
/// structs.
///
/// `MAP` and `VARIANT` are rejected, and not because of a mechanical limit. Feldera's map
/// is a `BTreeMap` and therefore canonically ordered, while a parquet map preserves
/// whatever order was written; a variant is encoded as JSON, and two equal variants can
/// render differently. On a table this connector does not administer there is no way to
/// enforce a canonical physical form, so equality would be silently wrong rather than
/// loudly unsupported. `ARRAY` is rejected for a weaker reason: it would work, but arrays
/// carry no statistics, so it is left until someone needs it.
pub fn validate_key_types(key_schema: &Relation) -> Result<(), String> {
    for field in &key_schema.fields {
        validate_key_column(&field.name.name(), &field.columntype)?;
    }
    Ok(())
}

fn validate_key_column(path: &str, columntype: &ColumnType) -> Result<(), String> {
    match columntype.typ {
        SqlType::Struct => {
            let Some(fields) = &columntype.fields else {
                return Err(format!(
                    "key column '{path}' has struct type with no field information"
                ));
            };
            for field in fields {
                validate_key_column(&format!("{path}.{}", field.name.name()), &field.columntype)?;
            }
            Ok(())
        }
        SqlType::Map | SqlType::Variant => Err(format!(
            "column '{path}' has type {:?}, which merge mode cannot use as part of a unique key. \
             Two values that Feldera considers equal can be stored differently in parquet \
             (map entry order, variant JSON rendering), so the connector could fail to find the \
             row it must supersede. Project a scalar surrogate into the view and index on that \
             instead.",
            columntype.typ
        )),
        SqlType::Array => Err(format!(
            "column '{path}' has type ARRAY, which merge mode does not yet support as part of a \
             unique key. Project a scalar surrogate into the view and index on that instead."
        )),
        SqlType::Null | SqlType::Interval(_) => Err(format!(
            "column '{path}' has type {:?}, which merge mode cannot use as part of a unique key.",
            columntype.typ
        )),
        _ => Ok(()),
    }
}

/// Dotted paths of the key's scalar leaves, in declaration order.
///
/// Delta writes min/max statistics per leaf, so these are the paths candidate pruning
/// looks up. A single scalar key column yields one path equal to its name; a `ROW` key
/// yields one path per leaf (`s.a`, `s.b`).
pub fn key_leaf_paths(key_schema: &Relation) -> Vec<String> {
    fn walk(path: &str, columntype: &ColumnType, out: &mut Vec<String>) {
        match (columntype.typ, &columntype.fields) {
            (SqlType::Struct, Some(fields)) => {
                for field in fields {
                    walk(
                        &format!("{path}.{}", field.name.name()),
                        &field.columntype,
                        out,
                    );
                }
            }
            _ => out.push(path.to_string()),
        }
    }

    let mut out = Vec::new();
    for field in &key_schema.fields {
        walk(&field.name.name(), &field.columntype, &mut out);
    }
    out
}

/// Encodes key values into comparable bytes.
///
/// Built once per connector against the target table's arrow schema. Both the lookup
/// chunk and the probe encode through the same instance, which is what makes their bytes
/// comparable.
#[derive(Debug)]
pub struct KeyEncoder {
    converter: RowConverter,
    /// Indices of the key columns within the table's arrow schema, in key declaration
    /// order. The probe projects a decoded batch through these so that column order in
    /// the file cannot change the encoding.
    column_indices: Vec<usize>,
    column_names: Vec<String>,
}

impl KeyEncoder {
    /// Build an encoder for `key_schema` against `table_schema`.
    ///
    /// Fails if a key column is missing from the table, or if its arrow type is one the
    /// row encoding cannot represent. The latter is a belt-and-braces check:
    /// [`validate_key_types`] should already have rejected such a key with a better
    /// message.
    pub fn new(key_schema: &Relation, table_schema: &ArrowSchema) -> AnyResult<Self> {
        let mut sort_fields = Vec::with_capacity(key_schema.fields.len());
        let mut column_indices = Vec::with_capacity(key_schema.fields.len());
        let mut column_names = Vec::with_capacity(key_schema.fields.len());

        for field in &key_schema.fields {
            let name = field.name.name();
            let index = table_schema.index_of(&name).map_err(|_| {
                anyhow!(
                    "key column '{name}' is not present in the target Delta table; \
                     the table schema must match the view schema"
                )
            })?;
            let arrow_field = table_schema.field(index);
            if !supported_by_row_encoding(arrow_field.data_type()) {
                bail!(
                    "key column '{name}' has type {} in the target Delta table, which cannot be \
                     encoded for key comparison",
                    arrow_field.data_type()
                );
            }
            sort_fields.push(SortField::new(arrow_field.data_type().clone()));
            column_indices.push(index);
            column_names.push(name);
        }

        let converter = RowConverter::new(sort_fields)
            .map_err(|e| anyhow!("unable to build key encoder: {e}"))?;

        Ok(Self {
            converter,
            column_indices,
            column_names,
        })
    }

    /// Column names of the key, in declaration order.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Indices of the key columns within the table's arrow schema.
    pub fn column_indices(&self) -> &[usize] {
        &self.column_indices
    }

    /// Encode the key columns of `batch`, which must carry them under their declared
    /// names.
    ///
    /// Lookup by name rather than by position: a probe reads a projected batch whose
    /// column order follows the parquet file, not the table schema.
    pub fn encode_batch(&self, batch: &RecordBatch) -> AnyResult<Rows> {
        let mut columns = Vec::with_capacity(self.column_names.len());
        for name in &self.column_names {
            let column = batch.column_by_name(name).ok_or_else(|| {
                anyhow!("key column '{name}' missing from batch read from the Delta table")
            })?;
            columns.push(column.clone());
        }
        self.encode_columns(&columns)
    }

    /// Encode key columns supplied directly, in declaration order.
    pub fn encode_columns(&self, columns: &[ArrayRef]) -> AnyResult<Rows> {
        self.converter
            .convert_columns(columns)
            .map_err(|e| anyhow!("unable to encode key: {e}"))
    }
}

/// Whether `arrow_row` can encode this type.
///
/// Mirrors the allowlist in `arrow_row`, which accepts nested structs and lists but not
/// maps. Kept explicit so a type slipping past [`validate_key_types`] fails with our
/// message rather than a panic inside the converter.
fn supported_by_row_encoding(data_type: &ArrowDataType) -> bool {
    match data_type {
        d if !d.is_nested() => true,
        ArrowDataType::Struct(fields) => fields
            .iter()
            .all(|f| supported_by_row_encoding(f.data_type())),
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::ListView(f)
        | ArrowDataType::LargeListView(f)
        | ArrowDataType::FixedSizeList(f, _) => supported_by_row_encoding(f.data_type()),
        _ => false,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field as ArrowField};
    use feldera_types::program_schema::{Field, SqlIdentifier};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn relation(fields: Vec<Field>) -> Relation {
        Relation {
            name: SqlIdentifier::new("k", false),
            fields,
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    fn scalar(name: &str, typ: SqlType) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ,
                nullable: false,
                precision: None,
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )
    }

    fn row_of(name: &str, inner: Vec<Field>) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ: SqlType::Struct,
                nullable: false,
                precision: None,
                scale: None,
                component: None,
                fields: Some(inner),
                key: None,
                value: None,
            },
        )
    }

    #[test]
    fn accepts_scalar_keys() {
        let rel = relation(vec![
            scalar("id", SqlType::BigInt),
            scalar("name", SqlType::Varchar),
        ]);
        assert!(validate_key_types(&rel).is_ok());
    }

    #[test]
    fn accepts_row_of_scalars() {
        let rel = relation(vec![row_of(
            "k",
            vec![scalar("a", SqlType::Int), scalar("b", SqlType::Varchar)],
        )]);
        assert!(validate_key_types(&rel).is_ok());
    }

    #[test]
    fn rejects_map_and_variant_by_name() {
        for typ in [SqlType::Map, SqlType::Variant] {
            let rel = relation(vec![scalar("k", typ)]);
            let err = validate_key_types(&rel).unwrap_err();
            assert!(err.contains("'k'"), "unexpected: {err}");
            assert!(err.contains("surrogate"), "unexpected: {err}");
        }
    }

    #[test]
    fn rejects_array() {
        let rel = relation(vec![scalar("k", SqlType::Array)]);
        assert!(validate_key_types(&rel).unwrap_err().contains("ARRAY"));
    }

    #[test]
    fn rejects_unsupported_type_nested_in_a_row() {
        let rel = relation(vec![row_of(
            "k",
            vec![scalar("a", SqlType::Int), scalar("bad", SqlType::Map)],
        )]);
        // The path names the leaf, not just the top-level column.
        assert!(validate_key_types(&rel).unwrap_err().contains("'k.bad'"));
    }

    #[test]
    fn leaf_paths_flatten_rows() {
        let rel = relation(vec![
            scalar("id", SqlType::BigInt),
            row_of(
                "k",
                vec![scalar("a", SqlType::Int), scalar("b", SqlType::Varchar)],
            ),
        ]);
        assert_eq!(key_leaf_paths(&rel), vec!["id", "k.a", "k.b"]);
    }

    #[test]
    fn encoder_round_trips_equality() {
        let rel = relation(vec![
            scalar("id", SqlType::BigInt),
            scalar("name", SqlType::Varchar),
        ]);
        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("other", DataType::Int64, true),
            ArrowField::new("name", DataType::Utf8, false),
        ]);
        let encoder = KeyEncoder::new(&rel, &schema).unwrap();

        // Key columns are located by name, so a batch whose column order differs from the
        // table schema still encodes identically.
        let a = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int64, false),
                ArrowField::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let b = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("name", DataType::Utf8, false),
                ArrowField::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["x", "z"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let ra = encoder.encode_batch(&a).unwrap();
        let rb = encoder.encode_batch(&b).unwrap();
        assert_eq!(ra.row(0), rb.row(0), "equal keys must encode identically");
        assert_ne!(ra.row(1), rb.row(1), "different keys must not collide");
    }

    #[test]
    fn encoder_rejects_key_column_missing_from_table() {
        let rel = relation(vec![scalar("id", SqlType::BigInt)]);
        let schema = ArrowSchema::new(vec![ArrowField::new("other", DataType::Int64, true)]);
        let err = KeyEncoder::new(&rel, &schema).unwrap_err().to_string();
        assert!(err.contains("not present in the target Delta table"), "{err}");
    }
}
