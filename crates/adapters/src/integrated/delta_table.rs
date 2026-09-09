mod deletion_vector;
mod input;
mod output;

#[cfg(test)]
mod test;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Schema as ArrowSchema, SchemaRef,
};
use feldera_types::serde_with_context::serde_config::{DecimalFormat, UuidFormat};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimestampFormat};
pub use input::DeltaTableInputEndpoint;
pub use output::DeltaTableWriter;
use std::sync::{Arc, Once};

/// The view counterpart of a string or binary type, or `None` for every other
/// type.
fn view_counterpart(data_type: &ArrowDataType) -> Option<ArrowDataType> {
    match data_type {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Some(ArrowDataType::Utf8View),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Some(ArrowDataType::BinaryView),
        _ => None,
    }
}

/// `data_type` with every string and binary type it contains, at any depth,
/// replaced by its view counterpart. Other types carry over unchanged.
fn view_data_type(data_type: &ArrowDataType) -> ArrowDataType {
    if let Some(view) = view_counterpart(data_type) {
        return view;
    }
    match data_type {
        ArrowDataType::Struct(fields) => {
            ArrowDataType::Struct(fields.iter().map(view_field).collect())
        }
        ArrowDataType::List(field) => ArrowDataType::List(view_field(field)),
        ArrowDataType::LargeList(field) => ArrowDataType::LargeList(view_field(field)),
        ArrowDataType::FixedSizeList(field, len) => {
            ArrowDataType::FixedSizeList(view_field(field), *len)
        }
        ArrowDataType::Map(field, sorted) => ArrowDataType::Map(view_field(field), *sorted),
        other => other.clone(),
    }
}

/// [`view_data_type`] applied to `field`, keeping its name, nullability and
/// metadata. The Parquet reader validates a supplied schema field by field, so
/// everything but the type must match what it would have produced.
fn view_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        ArrowField::new(
            field.name(),
            view_data_type(field.data_type()),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone()),
    )
}

/// An Arrow schema where every string and binary column, at
/// any depth, read as a view type.
///
/// A `Utf8`/`Binary` array addresses its values with 32-bit offsets, so one
/// decoded batch cannot hold more than 2 GiB of a single column, and the reader
/// fails with "index overflow decoding byte array" when a batch would. A view
/// array spreads values over several buffers and has no such ceiling. It is also
/// far cheaper for dictionary-encoded data: the decoder references the
/// dictionary page's buffers rather than copying each repeat, so a value
/// repeated across a batch costs one copy plus a 16-byte view per row.
///
/// The type exists to ensure that a schema that skipped the rewrite cannot reach a
/// reader.
///
/// Note: this doesn't apply to the initial snapshot: it reads through delta-rs's table
/// provider, which builds its own schema from the Delta log, and so keeps the
/// 32-bit types and their ceiling.
#[derive(Clone, Debug)]
pub(super) struct ReadSchema(SchemaRef);

impl ReadSchema {
    pub(super) fn new(schema: &ArrowSchema) -> Self {
        Self(Arc::new(
            ArrowSchema::new(
                schema
                    .fields()
                    .iter()
                    .map(view_field)
                    .collect::<Vec<FieldRef>>(),
            )
            .with_metadata(schema.metadata().clone()),
        ))
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.0
    }
}

static REGISTER_STORAGE_HANDLERS: Once = Once::new();

/// Register url handlers, so URL's like `s3://...` are recognized.
///
/// Runs initialization at most once per process.
pub fn register_storage_handlers() {
    REGISTER_STORAGE_HANDLERS.call_once(|| {
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);
        deltalake::gcp::register_handlers(None);
        deltalake::unity_catalog::register_handlers(None);
    });
}

pub fn delta_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Delta standard specifies that the timestamp type uses microseconds.
        // In reality, delta parquet files can represent timestamps as either
        // microseconds or nanoseconds.  Other options might be possible.
        // `serde_arrow` knows the correct type from the Arrow schema, and its
        // `Deserializer` implementation is nice enough to return the timestamp
        // formatted as string if the `Deserialize` implementation asks for it
        // (by calling `deserialize_str`), so we rely on that instead of trying
        // to deserialize the timestamp as an integer.  A better solution would
        // require a more flexible SqlSerdeConfig type that would specify a
        // schema per field.
        .with_timestamp_format(TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"))
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::String)
        // DeltaLake doesn't have a native UUID type. We assume that UUID
        // is represented as a string. If a different representation is used, the user
        // will have to deserialize into VARBINARY first. Alternatively, we can make
        // this configurable.
        .with_uuid_format(UuidFormat::String)
}
