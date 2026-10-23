//! Delta Lake tables for the Delta Lake suites: the rows of a relation in the form that a Delta
//! table holds them, and the rows that a table holds in the published form.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringBuilder};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::open_table;
use feldera_adapterlib::test::Updates;
use feldera_storage::tokio::TOKIO;
use feldera_types::program_schema::{Relation, SqlType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_variant_compute::{VariantArray, VariantArrayBuilder};
use parquet_variant_json::{JsonToVariant, VariantToJson};

use crate::format::parquet::{
    ArrowSchemaOptions, parquet_variant_arrow_type, relation_to_arrow_fields,
};
use crate::test::published_schema;

/// Returns the Arrow schema of a Delta table with the columns of `relation`, as the Delta Lake
/// output connector writes it by default: every column nullable, timestamps in UTC, and VARIANT
/// as a Parquet variant.
pub fn table_schema(relation: &Relation) -> SchemaRef {
    Arc::new(Schema::new(relation_to_arrow_fields(
        &relation.fields,
        ArrowSchemaOptions::new(true).with_parquet_variant(true),
    )))
}

/// Returns `rows`, in the published form of `relation`, in the form of [`table_schema`].
pub fn table_batch(relation: &Relation, rows: &RecordBatch) -> AnyResult<RecordBatch> {
    let schema = table_schema(relation);
    let columns = relation
        .fields
        .iter()
        .zip(schema.fields())
        .zip(rows.columns())
        .map(|((field, target), column)| {
            if field.columntype.typ == SqlType::Variant {
                json_to_variant(column.as_ref())
            } else {
                Ok(cast(column, target.data_type())?)
            }
        })
        .collect::<AnyResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Returns the rows of `batches`, which a Delta table or an ad hoc query holds, in the
/// published form of `relation`, each with weight 1. Columns of the batches that `relation`
/// lacks are left out.
pub fn published_rows(relation: &Relation, batches: &[RecordBatch]) -> AnyResult<Updates> {
    let schema = published_schema(relation);
    let mut converted = Vec::with_capacity(batches.len());
    for batch in batches {
        let columns = relation
            .fields
            .iter()
            .zip(schema.fields())
            .map(|(field, target)| {
                let name = field.name.name();
                let column = batch
                    .column_by_name(&name)
                    .ok_or_else(|| anyhow!("the Delta table has no column '{name}'"))?;
                // A Delta table holds a VARIANT as a Parquet variant, an ad hoc query as text.
                if field.columntype.typ == SqlType::Variant && column.data_type().is_nested() {
                    variant_to_json(column.as_ref())
                } else {
                    Ok(cast(column, target.data_type())?)
                }
            })
            .collect::<AnyResult<Vec<_>>>()?;
        converted.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    let rows = concat_batches(&schema, &converted)?;
    let weights = vec![1; rows.num_rows()];
    Updates::new(rows, weights)
}

/// Returns the rows of the Delta table at the local directory `path`, as a reader that applies
/// deletion vectors sees them, in the published form of `relation`.
pub fn table_rows(relation: &Relation, path: &str) -> AnyResult<Updates> {
    published_rows(relation, &table_batches(path)?)
}

/// Returns the batches of the Delta table at the local directory `path`, as a reader that
/// applies deletion vectors sees them, with the values exactly as the table holds them.
pub fn table_batches(path: &str) -> AnyResult<Vec<RecordBatch>> {
    let url =
        url::Url::from_file_path(path).map_err(|()| anyhow!("'{path}' is not an absolute path"))?;
    TOKIO.block_on(async move {
        let table = open_table(url).await?;
        let provider = table.table_provider().await?;
        Ok(SessionContext::new()
            .read_table(provider)?
            .collect()
            .await?)
    })
}

/// Returns the rows of the Parquet files at `paths`, files of a Delta table, in the published
/// form of `relation`, each with weight 1. A deletion vector does not hide a row here.
pub fn file_rows(relation: &Relation, paths: &[impl AsRef<Path>]) -> AnyResult<Updates> {
    let mut batches = Vec::new();
    for path in paths {
        let bytes = Bytes::from(std::fs::read(path)?);
        for batch in ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()? {
            batches.push(batch?);
        }
    }
    published_rows(relation, &batches)
}

/// Returns the Parquet files under the directory `path`, at any depth.
pub fn parquet_files(path: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(parquet_files(&path));
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            files.push(path);
        }
    }
    files
}

/// Returns `column`, VARIANT values as JSON text, as Parquet variants.
fn json_to_variant(column: &dyn Array) -> AnyResult<ArrayRef> {
    let texts = column.as_string::<i32>();
    let mut builder = VariantArrayBuilder::new(texts.len());
    for index in 0..texts.len() {
        if texts.is_null(index) {
            builder.append_null();
        } else {
            builder.append_json(texts.value(index))?;
        }
    }
    let variants = ArrayRef::from(builder.build());
    Ok(cast(&variants, &parquet_variant_arrow_type())?)
}

/// Returns `column`, Parquet variants, as VARIANT values in JSON text.
fn variant_to_json(column: &dyn Array) -> AnyResult<ArrayRef> {
    let variants = VariantArray::try_new(column)?;
    let mut texts = StringBuilder::new();
    for index in 0..variants.len() {
        if variants.is_null(index) {
            texts.append_null();
        } else {
            texts.append_value(variants.value(index).to_json_string()?);
        }
    }
    Ok(Arc::new(texts.finish()))
}
