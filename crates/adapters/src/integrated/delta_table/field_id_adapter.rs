//! Resolving a [`ListingTable`]'s columns by Delta field id.
//!
//! Under `delta.columnMapping.mode = 'id'` the log names a column `col-<id>`,
//! but an Iceberg writer leaves the logical name in the data file. DataFusion
//! pairs a file's columns with the table schema by name alone, so such a column
//! reads as NULL. This adapter rewrites each column reference to the name the
//! file actually uses, found by field id.
//!
//! [`ListingTable`]: datafusion::datasource::listing::ListingTable

use crate::integrated::delta_table::deletion_vector::{
    field_index_by_id, realign_array, source_index,
};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastColumnExpr, Column, lit};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hands every file scanned by one [`ListingTable`] its own [`FieldIdAdapter`].
///
/// [`ListingTable`]: datafusion::datasource::listing::ListingTable
#[derive(Debug)]
pub(super) struct FieldIdAdapterFactory {
    logical_names: Arc<HashMap<String, String>>,
}

impl FieldIdAdapterFactory {
    /// `column_mapping` is the connector's logical-to-physical name pairs; the
    /// adapter reads them backwards, to name a column in an error the way the
    /// SQL schema does rather than as the opaque `col-<id>` on disk.
    pub(super) fn new(column_mapping: &[(String, String)]) -> Self {
        Self {
            logical_names: Arc::new(
                column_mapping
                    .iter()
                    .map(|(logical, physical)| (physical.clone(), logical.clone()))
                    .collect(),
            ),
        }
    }
}

impl PhysicalExprAdapterFactory for FieldIdAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(FieldIdAdapter {
            logical_file_schema,
            physical_file_schema,
            logical_names: Arc::clone(&self.logical_names),
        }))
    }
}

/// Reads one file's columns as the Delta table's schema declares them.
#[derive(Debug)]
struct FieldIdAdapter {
    /// The table schema, minus partition columns: physical names, view types.
    logical_file_schema: SchemaRef,
    /// The schema the file's own footer declares.
    physical_file_schema: SchemaRef,
    /// Physical-to-logical column names, for errors the user reads.
    logical_names: Arc<HashMap<String, String>>,
}

impl PhysicalExprAdapter for FieldIdAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let by_id = field_index_by_id(self.physical_file_schema.fields());
        expr.transform(|expr| match expr.as_any().downcast_ref::<Column>() {
            Some(column) => self.rewrite_column(Arc::clone(&expr), column, &by_id),
            None => Ok(Transformed::no(expr)),
        })
        .data()
    }
}

impl FieldIdAdapter {
    /// Point `column` at the file's own column of the same field id, converting
    /// it to the type the schema declares.
    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
        by_id: &HashMap<&str, usize>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // A rewrite of ours may have introduced a reference to a column of the
        // file that the table's schema does not name. Look its index up in the
        // file rather than keep the one it carries: a reference left at the
        // wrong index reads a neighbor's values and says nothing.
        let Ok(want) = self.logical_file_schema.field_with_name(column.name()) else {
            let Ok(index) = self.physical_file_schema.index_of(column.name()) else {
                return exec_err!(
                    "Delta file reader: column '{}' is in neither the Delta table's schema \
                     nor the data file being scanned",
                    self.logical_name(column.name())
                );
            };
            if index == column.index() {
                return Ok(Transformed::no(expr));
            }
            return Ok(Transformed::yes(Arc::new(Column::new(
                column.name(),
                index,
            ))));
        };
        let Some(index) = source_index(&self.physical_file_schema, by_id, want) else {
            // A column added after the file was written reads as NULL, which is
            // what it holds for those rows.
            if !want.is_nullable() {
                return exec_err!(
                    "Delta file reader: cannot read column '{}': the Delta table's schema \
                     declares it NOT NULL, but the data file being scanned does not carry it",
                    self.logical_name(want.name())
                );
            }
            return Ok(Transformed::yes(lit(
                ScalarValue::Null.cast_to(want.data_type())?
            )));
        };

        let source = self.physical_file_schema.field(index);
        if source.data_type() == want.data_type() {
            // The file names and types the column as the schema does; the
            // reference already points at it.
            if source.name() == column.name() && index == column.index() {
                return Ok(Transformed::no(expr));
            }
            return Ok(Transformed::yes(Arc::new(Column::new(
                source.name(),
                index,
            ))));
        }
        let found = Arc::new(Column::new(source.name(), index));
        if needs_field_id_match(want.data_type()) {
            return Ok(Transformed::yes(Arc::new(RealignExpr {
                input: found,
                target: Arc::new(want.clone()),
                column: self.logical_name(want.name()).to_string(),
            })));
        }
        // A plain column keeps DataFusion's own cast, which the simplifier can
        // move to the literal side of a predicate and so still prune row groups.
        Ok(Transformed::yes(Arc::new(CastColumnExpr::new(
            found,
            Arc::new(source.clone()),
            Arc::new(want.clone()),
            None,
        ))))
    }

    /// The name the SQL schema gives the column stored as `physical`.
    fn logical_name<'a>(&'a self, physical: &'a str) -> &'a str {
        self.logical_names
            .get(physical)
            .map_or(physical, String::as_str)
    }
}

/// Must a column of this type be paired by field id rather than cast?
///
/// DataFusion's cast pairs struct children by name and fails outright when none
/// overlap, which is every column-mapped struct. A container may hide one.
fn needs_field_id_match(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

/// Reads a file column as `target`, pairing nested struct fields by field id.
#[derive(Debug, Clone, Eq)]
struct RealignExpr {
    input: Arc<dyn PhysicalExpr>,
    target: FieldRef,
    /// The column's name in the SQL schema, for errors. A listing's reader is
    /// given no path, so an error here cannot name the file as well.
    column: String,
}

// `Arc<dyn PhysicalExpr>` derives neither, so spell both out; DataFusion's own
// expressions do the same.
impl PartialEq for RealignExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.target.eq(&other.target) && self.column == other.column
    }
}

impl Hash for RealignExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.target.hash(state);
        self.column.hash(state);
    }
}

impl fmt::Display for RealignExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "realign({}, {})", self.input, self.target.data_type())
    }
}

impl PhysicalExpr for RealignExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target.is_nullable())
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(realign_array(
            &array,
            self.target.data_type(),
            None,
            &self.column,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Self {
                input: children.remove(0),
                target: Arc::clone(&self.target),
                column: self.column.clone(),
            })),
            n => Err(DataFusionError::Internal(format!(
                "realign takes one child, given {n}"
            ))),
        }
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};
    use datafusion::physical_expr::expressions::Literal;

    /// A read-schema field: the physical name the log gives it, and its mapping id.
    fn mapped(name: &str, id: &str, data_type: DataType, nullable: bool) -> Field {
        Field::new(name, data_type, nullable)
            .with_metadata([("delta.columnMapping.id".into(), id.into())].into())
    }

    /// A data-file field: whatever name its writer chose, and its Parquet field id.
    fn written(name: &str, id: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
            .with_metadata([("PARQUET:field_id".into(), id.into())].into())
    }

    /// Rewrite `column` against the pair of schemas.
    fn rewrite(
        logical: Vec<Field>,
        file: Vec<Field>,
        column: Column,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        FieldIdAdapterFactory::new(&[])
            .create(Arc::new(Schema::new(logical)), Arc::new(Schema::new(file)))?
            .rewrite(Arc::new(column))
    }

    /// Rewrite a reference to the first column of `logical` against `file`.
    fn rewrite_first(logical: Vec<Field>, file: Vec<Field>) -> Result<Arc<dyn PhysicalExpr>> {
        let name = logical[0].name().clone();
        rewrite(logical, file, Column::new(&name, 0))
    }

    /// The Iceberg case: the file names the column logically, so only its field
    /// id pairs the two. Without this the column reads as NULL.
    #[test]
    fn a_logical_name_is_found_by_field_id() {
        let expr = rewrite_first(
            vec![mapped("col-1", "1", DataType::Utf8, true)],
            vec![written("label", "1", DataType::Utf8)],
        )
        .unwrap();
        let column = expr.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(column.name(), "label");
    }

    /// A column added after the file was written holds nothing for those rows.
    #[test]
    fn a_column_the_file_lacks_reads_null() {
        let expr = rewrite_first(
            vec![mapped("col-9", "9", DataType::Utf8, true)],
            vec![written("label", "1", DataType::Utf8)],
        )
        .unwrap();
        assert!(expr.as_any().downcast_ref::<Literal>().is_some());
    }

    /// NULL is not an answer the table accepts, so say so rather than fail a
    /// batch later with an Arrow error. The message names the column as the SQL
    /// schema does, not by the `col-<id>` it is stored under.
    #[test]
    fn a_missing_not_null_column_is_an_error() {
        let logical = Arc::new(Schema::new(vec![mapped(
            "col-9",
            "9",
            DataType::Utf8,
            false,
        )]));
        let file = Arc::new(Schema::new(vec![written("label", "1", DataType::Utf8)]));
        let error = FieldIdAdapterFactory::new(&[("amount".into(), "col-9".into())])
            .create(logical, file)
            .unwrap()
            .rewrite(Arc::new(Column::new("col-9", 0)))
            .unwrap_err()
            .to_string();
        assert!(error.contains("column 'amount'"), "{error}");
        assert!(error.contains("NOT NULL"), "{error}");
    }

    /// A struct's children are column-mapped too, and DataFusion's cast pairs
    /// them by name, which fails outright when none of the names overlap.
    #[test]
    fn a_struct_is_realigned_rather_than_cast() {
        let file_child = Arc::new(written("ccy", "2", DataType::Utf8));
        let want_child = Arc::new(mapped("col-2", "2", DataType::Utf8View, true));
        let expr = rewrite_first(
            vec![mapped(
                "col-1",
                "1",
                DataType::Struct(Fields::from(vec![want_child])),
                true,
            )],
            vec![written(
                "price",
                "1",
                DataType::Struct(Fields::from(vec![file_child])),
            )],
        )
        .unwrap();
        assert!(expr.as_any().downcast_ref::<RealignExpr>().is_some());
    }

    /// A scalar keeps DataFusion's own cast, which the simplifier can move to
    /// the literal side of a predicate, so the file's row groups still prune.
    #[test]
    fn a_scalar_keeps_datafusions_cast() {
        let expr = rewrite_first(
            vec![mapped("col-1", "1", DataType::Utf8View, true)],
            vec![written("label", "1", DataType::Utf8)],
        )
        .unwrap();
        assert!(expr.as_any().downcast_ref::<CastColumnExpr>().is_some());
    }

    /// Delta names a file's columns the way the log does, and the rewrite must
    /// leave that reference exactly as it found it.
    #[test]
    fn a_matching_name_is_left_alone() {
        let expr = rewrite_first(
            vec![mapped("col-1", "1", DataType::Utf8, true)],
            vec![written("col-1", "1", DataType::Utf8)],
        )
        .unwrap();
        let column = expr.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("col-1", 0));
    }

    /// A reference the table's schema does not name belongs to the file, which
    /// numbers its columns its own way. Reading it at the index it arrives with
    /// would return a neighbor's values and say nothing.
    #[test]
    fn a_file_only_column_is_read_at_its_index_in_the_file() {
        let expr = rewrite(
            vec![mapped("col-1", "1", DataType::Utf8, true)],
            vec![
                written("label", "1", DataType::Utf8),
                written("row_index", "2", DataType::Int64),
            ],
            Column::new("row_index", 0),
        )
        .unwrap();
        let column = expr.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("row_index", 1));
    }

    /// Neither schema names the column, so no index is right. Fail rather than
    /// read whichever column the stale index lands on.
    #[test]
    fn a_column_neither_schema_names_is_an_error() {
        let error = rewrite(
            vec![mapped("col-1", "1", DataType::Utf8, true)],
            vec![written("label", "1", DataType::Utf8)],
            Column::new("absent", 0),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("'absent' is in neither"),
            "the error must name the column: {error}"
        );
    }
}
