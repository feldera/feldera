//! Key validation and encoding for merge mode.
//!
//! Merge mode locates the row to supersede by comparing a key Feldera holds against the same
//! value read back from parquet. Both sides go through [`arrow_row::RowConverter`], which
//! encodes a value as bytes that compare the way the value does, so the probe is a byte
//! comparison and the lookup chunk is a sorted byte buffer.
//!
//! A type is usable as a key only when equality survives that round trip. That is a per-type
//! property, so [`validate_key_types`] names the types it rejects rather than testing for
//! nesting.

use anyhow::{Result as AnyResult, anyhow, bail};
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow::row::{RowConverter, Rows, SortField};
use feldera_types::program_schema::{ColumnType, Relation, SqlType};

/// Reject key types whose round trip through parquet does not preserve Feldera's equality.
///
/// `ROW` is fine when every leaf is: it is a composite key in disguise, and Delta writes
/// statistics per leaf path.
///
/// `MAP` and `VARIANT` have no canonical physical form -- a parquet map keeps whatever entry
/// order was written, a variant renders as JSON -- so on a table we do not administer,
/// equality would be silently wrong rather than loudly unsupported. `ARRAY` would work but
/// carries no statistics, so it waits until someone needs it.
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
/// Delta writes min/max statistics per leaf, so these are the paths pruning looks up: a
/// scalar key yields its own name, a `ROW` key yields one path per leaf (`s.a`, `s.b`).
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
/// Built once per connector. The lookup chunk and the probe encode through the same
/// instance, which is what makes their bytes comparable.
#[derive(Debug)]
pub struct KeyEncoder {
    converter: RowConverter,
    /// Key column indices in the table's arrow schema, in declaration order. The probe
    /// projects through these, so a file's column order cannot change the encoding.
    column_indices: Vec<usize>,
    column_names: Vec<String>,
    /// Arrow type the converter expects per column, for [`Self::encode_columns`] to check.
    column_types: Vec<ArrowDataType>,
}

impl KeyEncoder {
    /// Build an encoder for `key_schema` against `table_schema`.
    ///
    /// Fails if a key column is missing from the table, or if the row encoding cannot
    /// represent its type -- a backstop, since [`validate_key_types`] rejects those earlier
    /// with a better message.
    pub fn new(key_schema: &Relation, table_schema: &ArrowSchema) -> AnyResult<Self> {
        let mut sort_fields = Vec::with_capacity(key_schema.fields.len());
        let mut column_indices = Vec::with_capacity(key_schema.fields.len());
        let mut column_names = Vec::with_capacity(key_schema.fields.len());
        let mut column_types = Vec::with_capacity(key_schema.fields.len());

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
            column_types.push(arrow_field.data_type().clone());
        }

        let converter = RowConverter::new(sort_fields)
            .map_err(|e| anyhow!("unable to build key encoder: {e}"))?;

        Ok(Self {
            converter,
            column_indices,
            column_names,
            column_types,
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

    /// Encode the key columns of `batch`, found by name: a probe reads a projected batch
    /// whose column order follows the parquet file, not the table schema.
    #[cfg(test)]
    pub fn encode_batch(&self, batch: &RecordBatch) -> AnyResult<Rows> {
        self.encode_columns(&self.columns_of(batch)?)
    }

    /// The key columns of `batch`, in declaration order. Separate from [`Self::encode_batch`]
    /// so a caller needing both the encoded key and the columns projects once.
    pub fn columns_of(&self, batch: &RecordBatch) -> AnyResult<Vec<ArrayRef>> {
        let mut columns = Vec::with_capacity(self.column_names.len());
        for name in &self.column_names {
            let column = batch.column_by_name(name).ok_or_else(|| {
                anyhow!("key column '{name}' missing from batch read from the Delta table")
            })?;
            columns.push(column.clone());
        }
        Ok(columns)
    }

    /// Encode key columns supplied directly, in declaration order.
    pub fn encode_columns(&self, columns: &[ArrayRef]) -> AnyResult<Rows> {
        let columns = self.cast_to_declared_types(columns)?;
        self.converter
            .convert_columns(&columns)
            .map_err(|e| anyhow!("unable to encode key: {e}"))
    }

    /// Cast each column to the type the converter expects.
    ///
    /// Feldera writes a BINARY column as arrow LargeBinary, while the Delta table declares
    /// it as Binary. A Parquet file stores the arrow schema of whoever wrote it, so the
    /// connector reads its own files back as LargeBinary. Casting to Binary leaves the bytes
    /// alone, so both sides encode to the same key.
    fn cast_to_declared_types(&self, columns: &[ArrayRef]) -> AnyResult<Vec<ArrayRef>> {
        if columns.len() != self.column_types.len() {
            return Err(anyhow!(
                "the key has {} column(s) but {} were supplied to encode",
                self.column_types.len(),
                columns.len()
            ));
        }

        let mut matched = Vec::with_capacity(columns.len());
        for ((column, declared), name) in columns
            .iter()
            .zip(&self.column_types)
            .zip(&self.column_names)
        {
            if column.data_type() == declared {
                matched.push(column.clone());
            } else if cast_preserves_values(column.data_type(), declared) {
                matched.push(cast(column.as_ref(), declared).map_err(|e| {
                    anyhow!("unable to cast key column '{name}' to {declared}: {e}")
                })?);
            } else {
                // Fail rather than cast: arrow replaces a value that does not fit with null,
                // and a null key would match the wrong row.
                return Err(anyhow!(
                    "key column '{name}' is {} in the data file but {declared} in the target \
                     Delta table. The connector will not convert between these types, because \
                     a value changed by the conversion would supersede the wrong row.",
                    column.data_type()
                ));
            }
        }
        Ok(matched)
    }
}

/// Whether casting `from` to `to` leaves every value unchanged.
///
/// Only LargeBinary to Binary qualifies. The bytes are the same and only the offset width
/// shrinks, and a value too large for a 32-bit offset fails the cast instead of being
/// truncated.
fn cast_preserves_values(from: &ArrowDataType, to: &ArrowDataType) -> bool {
    match (from, to) {
        (ArrowDataType::LargeBinary, ArrowDataType::Binary) => true,
        // A ROW key is a composite key, so check its leaves the same way.
        (ArrowDataType::Struct(from), ArrowDataType::Struct(to)) => {
            from.len() == to.len()
                && from.iter().zip(to.iter()).all(|(from, to)| {
                    from.name() == to.name()
                        && (from.data_type() == to.data_type()
                            || cast_preserves_values(from.data_type(), to.data_type()))
                })
        }
        _ => false,
    }
}

/// Whether any of these key values is null, at any depth.
///
/// Recurses into `ROW`: a struct that is itself non-null can still hold a null leaf.
pub fn contains_null(columns: &[ArrayRef]) -> bool {
    fn walk(array: &dyn Array) -> bool {
        if array.null_count() > 0 {
            return true;
        }
        match array.as_any().downcast_ref::<StructArray>() {
            Some(structs) => structs.columns().iter().any(|c| walk(c.as_ref())),
            None => false,
        }
    }

    columns.iter().any(|c| walk(c.as_ref()))
}

/// Whether `arrow_row` can encode this type: nested structs and lists yes, maps no. Explicit
/// so a type slipping past [`validate_key_types`] gets our error, not a panic in the converter.
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

    /// A struct that is itself non-null can still hold a null leaf, which pruning must see.
    #[test]
    fn contains_null_sees_a_null_leaf_inside_a_row() {
        let present: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        assert!(!contains_null(std::slice::from_ref(&present)));

        let absent: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64), None]));
        assert!(contains_null(std::slice::from_ref(&absent)));

        let row = |leaf: ArrayRef| -> ArrayRef {
            Arc::new(StructArray::from(vec![(
                Arc::new(ArrowField::new("leaf", DataType::Int64, true)),
                leaf,
            )]))
        };
        assert!(!contains_null(&[row(present)]));
        assert!(
            contains_null(&[row(absent)]),
            "the struct is non-null but its leaf is not"
        );
    }

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

    /// A BINARY key is declared Binary but reads back from Parquet as LargeBinary. Both must
    /// encode to the same bytes, or the lookup never finds the row to supersede.
    #[test]
    fn a_wider_column_encodes_the_same_as_the_declared_one() {
        use arrow::array::{BinaryArray, LargeBinaryArray};

        let rel = relation(vec![scalar("b", SqlType::Varbinary)]);
        let schema = ArrowSchema::new(vec![ArrowField::new("b", DataType::Binary, false)]);
        let encoder = KeyEncoder::new(&rel, &schema).unwrap();

        let values: Vec<&[u8]> = vec![b"one", b"two"];
        let declared: ArrayRef = Arc::new(BinaryArray::from(values.clone()));
        let wider: ArrayRef = Arc::new(LargeBinaryArray::from(values));

        let a = encoder.encode_columns(&[declared]).unwrap();
        let b = encoder.encode_columns(&[wider]).unwrap();
        assert_eq!(a.row(0), b.row(0), "the same value must encode identically");
        assert_eq!(a.row(1), b.row(1));
        assert_ne!(a.row(0), b.row(1), "different values must not collide");
    }

    /// The same, one level down, since a ROW key can hold a BINARY leaf.
    #[test]
    fn a_wider_leaf_inside_a_row_encodes_the_same() {
        use arrow::array::{BinaryArray, LargeBinaryArray};
        use arrow::datatypes::Fields;

        let leaf = |t: DataType| Arc::new(ArrowField::new("b", t, false));
        let declared_type = DataType::Struct(Fields::from(vec![leaf(DataType::Binary)]));
        let rel = relation(vec![row_of("r", vec![scalar("b", SqlType::Varbinary)])]);
        let schema = ArrowSchema::new(vec![ArrowField::new("r", declared_type, false)]);
        let encoder = KeyEncoder::new(&rel, &schema).unwrap();

        let values: Vec<&[u8]> = vec![b"one"];
        let declared: ArrayRef = Arc::new(StructArray::from(vec![(
            leaf(DataType::Binary),
            Arc::new(BinaryArray::from(values.clone())) as ArrayRef,
        )]));
        let wider: ArrayRef = Arc::new(StructArray::from(vec![(
            leaf(DataType::LargeBinary),
            Arc::new(LargeBinaryArray::from(values)) as ArrayRef,
        )]));

        assert_eq!(
            encoder.encode_columns(&[declared]).unwrap().row(0),
            encoder.encode_columns(&[wider]).unwrap().row(0)
        );
    }

    /// Any other type mismatch must fail rather than cast: arrow replaces an out-of-range
    /// value with null, and a null key would match the wrong row.
    #[test]
    fn a_mismatch_that_could_change_a_value_is_refused() {
        let rel = relation(vec![scalar("id", SqlType::Int)]);
        let schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let encoder = KeyEncoder::new(&rel, &schema).unwrap();

        let too_wide: ArrayRef = Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1]));
        let err = encoder.encode_columns(&[too_wide]).unwrap_err().to_string();
        assert!(
            err.contains("will not convert between these types"),
            "{err}"
        );
        assert!(
            err.contains("'id'"),
            "the error must name the column: {err}"
        );
    }

    #[test]
    fn encoder_rejects_key_column_missing_from_table() {
        let rel = relation(vec![scalar("id", SqlType::BigInt)]);
        let schema = ArrowSchema::new(vec![ArrowField::new("other", DataType::Int64, true)]);
        let err = KeyEncoder::new(&rel, &schema).unwrap_err().to_string();
        assert!(
            err.contains("not present in the target Delta table"),
            "{err}"
        );
    }
}
