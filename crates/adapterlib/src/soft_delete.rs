//! Soft deletes: ingest deletions as insertions.
//!
//! An input connector configured with `soft_delete` pushes every record it
//! receives to the table as an insertion, including records that the input
//! stream marks as deleted, and reports the original polarity of each record in
//! the [`IS_DELETE_ATTRIBUTE`] metadata attribute.  The table then contains the
//! entire history of the input stream instead of tracking its current contents.
//!
//! The transformation wraps the input handle of the table, so it applies to
//! every data format and to integrated connectors alike:
//!
//! ```text
//! transport → parser → SoftDeleteStream → table
//! ```

use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::Arc;

use anyhow::{Result as AnyResult, bail};
#[cfg(feature = "with-avro")]
use apache_avro::{Schema as AvroSchema, types::Value as AvroValue};
use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use dbsp::operator::StagedBuffers;
use feldera_sqllib::{SqlString, Variant};
use feldera_types::serde_with_context::SqlSerdeConfig;

use crate::catalog::{ArrowStream, DeCollectionHandle, DeCollectionStream, RecordFormat};
#[cfg(feature = "with-avro")]
use crate::catalog::{AvroSchemaRefs, AvroStream};
use crate::errors::controller::ControllerError;
use crate::format::{BufferSize, InputBuffer};

/// Metadata attribute that reports the original polarity of a record ingested
/// by a connector with soft deletes enabled: `true` for a deleted record,
/// absent for an inserted one.
pub const IS_DELETE_ATTRIBUTE: &str = "is_delete";

/// Builds the record metadata that a soft-delete connector attaches to a
/// deleted record: the metadata produced by the connector and the parser plus
/// `is_delete: true`.
struct DeleteMetadata {
    /// The `is_delete` attribute name, built once.
    key: Variant,

    /// Metadata for a connector that reports no attributes of its own.
    without_attributes: Variant,

    /// The most recent (connector metadata, extended metadata) pair.
    ///
    /// A connector attaches the same metadata to all records of an input
    /// message, so caching the last result means one allocation per message
    /// rather than one per deleted record.  The cache holds on to the connector
    /// metadata it was computed from, which keeps that allocation alive and the
    /// pointer comparison in [`Self::extend`] meaningful.
    last: Option<(Arc<BTreeMap<Variant, Variant>>, Variant)>,
}

impl DeleteMetadata {
    fn new() -> Self {
        let key = Variant::String(SqlString::from(IS_DELETE_ATTRIBUTE));
        Self {
            without_attributes: Variant::Map(Arc::new(BTreeMap::from([(
                key.clone(),
                Variant::Boolean(true),
            )]))),
            key,
            last: None,
        }
    }

    /// Returns `metadata` extended with `is_delete: true`.
    fn extend(&mut self, metadata: &Option<Variant>) -> Option<Variant> {
        let Some(Variant::Map(attributes)) = metadata else {
            // Connectors and parsers report metadata as a map of attributes, so
            // anything else means there is nothing to preserve.
            debug_assert!(metadata.is_none(), "record metadata is not a map");
            return Some(self.without_attributes.clone());
        };

        if let Some((cached, extended)) = &self.last
            && Arc::ptr_eq(cached, attributes)
        {
            return Some(extended.clone());
        }

        let mut extended = (**attributes).clone();
        extended.insert(self.key.clone(), Variant::Boolean(true));
        let extended = Variant::Map(Arc::new(extended));
        self.last = Some((attributes.clone(), extended.clone()));

        Some(extended)
    }
}

/// Wraps the input handle of a table, turning the deletions pushed by one
/// connector into insertions.  See the [module documentation](self).
pub struct SoftDeleteHandle {
    inner: Box<dyn DeCollectionHandle>,
}

impl SoftDeleteHandle {
    pub fn new(inner: Box<dyn DeCollectionHandle>) -> Self {
        Self { inner }
    }
}

impl DeCollectionHandle for SoftDeleteHandle {
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        Ok(Box::new(SoftDeleteStream::new(
            self.inner.configure_deserializer(record_format)?,
        )))
    }

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        Ok(Box::new(SoftDeleteArrowStream::new(
            self.inner.configure_arrow_deserializer(config)?,
        )))
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError> {
        Ok(Box::new(SoftDeleteAvroStream::new(
            self.inner.configure_avro_deserializer()?,
        )))
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(Self::new(self.inner.fork()))
    }
}

/// [`DeCollectionStream`] that ingests deletions as insertions.
struct SoftDeleteStream {
    inner: Box<dyn DeCollectionStream>,
    metadata: DeleteMetadata,
}

impl SoftDeleteStream {
    fn new(inner: Box<dyn DeCollectionStream>) -> Self {
        Self {
            inner,
            metadata: DeleteMetadata::new(),
        }
    }
}

impl DeCollectionStream for SoftDeleteStream {
    fn insert(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        self.inner.insert(data, metadata)
    }

    fn delete(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let metadata = self.metadata.extend(metadata);
        self.inner.insert(data, &metadata)
    }

    fn update(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        self.inner.update(data, metadata)
    }

    fn reserve(&mut self, reservation: usize) {
        self.inner.reserve(reservation)
    }

    fn truncate(&mut self, len: usize) {
        self.inner.truncate(len)
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.inner.stage(buffers)
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.inner.fork()))
    }
}

impl InputBuffer for SoftDeleteStream {
    fn flush(&mut self) {
        self.inner.flush()
    }

    fn len(&self) -> BufferSize {
        self.inner.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.inner.hash(hasher)
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.inner.take_some(n)
    }
}

/// [`ArrowStream`] that ingests deletions as insertions.
struct SoftDeleteArrowStream {
    inner: Box<dyn ArrowStream>,
    metadata: DeleteMetadata,
}

impl SoftDeleteArrowStream {
    fn new(inner: Box<dyn ArrowStream>) -> Self {
        Self {
            inner,
            metadata: DeleteMetadata::new(),
        }
    }
}

impl ArrowStream for SoftDeleteArrowStream {
    fn insert(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        self.inner.insert(data, metadata)
    }

    fn delete(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        let metadata = self.metadata.extend(metadata);
        self.inner.insert(data, &metadata)
    }

    fn insert_with_polarities(
        &mut self,
        data: &RecordBatch,
        polarities: &[bool],
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        if polarities.len() != data.num_rows() {
            bail!(
                "insert_with_polarities: RecordBatch contains {} records, but 'polarities' array has length {}",
                data.num_rows(),
                polarities.len()
            );
        }

        let deleted = polarities.iter().filter(|polarity| !**polarity).count();
        if deleted == 0 {
            return self.inner.insert(data, metadata);
        }
        if deleted == polarities.len() {
            let metadata = self.metadata.extend(metadata);
            return self.inner.insert(data, &metadata);
        }

        // Inserted and deleted records in the batch need different metadata,
        // but metadata applies to an entire batch, so split the batch in two.
        // Splitting reorders the records of the batch, which is harmless: a
        // table with soft deletes cannot have a primary key, so its updates
        // commute.
        let inserted_mask = BooleanArray::from(polarities.to_vec());
        let deleted_mask = BooleanArray::from(
            polarities
                .iter()
                .map(|polarity| !polarity)
                .collect::<Vec<_>>(),
        );

        self.inner
            .insert(&filter_record_batch(data, &inserted_mask)?, metadata)?;

        let metadata = self.metadata.extend(metadata);
        self.inner
            .insert(&filter_record_batch(data, &deleted_mask)?, &metadata)
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(self.inner.fork()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.inner.stage(buffers)
    }
}

impl InputBuffer for SoftDeleteArrowStream {
    fn flush(&mut self) {
        self.inner.flush()
    }

    fn len(&self) -> BufferSize {
        self.inner.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.inner.hash(hasher)
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.inner.take_some(n)
    }
}

/// [`AvroStream`] that ingests deletions as insertions.
#[cfg(feature = "with-avro")]
struct SoftDeleteAvroStream {
    inner: Box<dyn AvroStream>,
    metadata: DeleteMetadata,
}

#[cfg(feature = "with-avro")]
impl SoftDeleteAvroStream {
    fn new(inner: Box<dyn AvroStream>) -> Self {
        Self {
            inner,
            metadata: DeleteMetadata::new(),
        }
    }
}

#[cfg(feature = "with-avro")]
impl AvroStream for SoftDeleteAvroStream {
    fn insert(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        self.inner.insert(data, schema, refs, n_bytes, metadata)
    }

    fn delete(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        let metadata = self.metadata.extend(metadata);
        self.inner.insert(data, schema, refs, n_bytes, &metadata)
    }

    fn fork(&self) -> Box<dyn AvroStream> {
        Box::new(Self::new(self.inner.fork()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.inner.stage(buffers)
    }
}

#[cfg(feature = "with-avro")]
impl InputBuffer for SoftDeleteAvroStream {
    fn flush(&mut self) {
        self.inner.flush()
    }

    fn len(&self) -> BufferSize {
        self.inner.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.inner.hash(hasher)
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.inner.take_some(n)
    }
}

#[cfg(test)]
mod test {
    use super::{DeleteMetadata, IS_DELETE_ATTRIBUTE};
    use crate::ConnectorMetadata;
    use feldera_sqllib::{SqlString, Variant};

    fn is_delete(metadata: &Option<Variant>) -> Variant {
        metadata.as_ref().unwrap().index_string(IS_DELETE_ATTRIBUTE)
    }

    /// A connector that reports no metadata of its own still reports the
    /// polarity of a deleted record.
    #[test]
    fn extends_absent_metadata() {
        let mut delete_metadata = DeleteMetadata::new();

        let extended = delete_metadata.extend(&None);
        assert_eq!(is_delete(&extended), Variant::Boolean(true));
    }

    /// Extending metadata preserves the attributes reported by the connector.
    #[test]
    fn preserves_connector_attributes() {
        let mut connector_metadata = ConnectorMetadata::new();
        connector_metadata.insert("kafka_offset", Variant::BigInt(17));
        let metadata = Some(Variant::from(connector_metadata));

        let mut delete_metadata = DeleteMetadata::new();
        let extended = delete_metadata.extend(&metadata);

        assert_eq!(is_delete(&extended), Variant::Boolean(true));
        assert_eq!(
            extended.as_ref().unwrap().index_string("kafka_offset"),
            Variant::BigInt(17)
        );

        // The connector metadata itself is unchanged: an inserted record that
        // shares it must not report a polarity.
        assert_eq!(
            is_delete(&metadata),
            Variant::SqlNull,
            "extending metadata must not modify the connector's copy"
        );
    }

    /// All records of a message share one metadata object, so extending it
    /// repeatedly returns the cached result rather than a fresh allocation.
    #[test]
    fn caches_the_last_result() {
        let mut connector_metadata = ConnectorMetadata::new();
        connector_metadata.insert("topic", Variant::String(SqlString::from("events")));
        let metadata = Some(Variant::from(connector_metadata));

        let mut delete_metadata = DeleteMetadata::new();
        let first = delete_metadata.extend(&metadata);
        let second = delete_metadata.extend(&metadata);

        assert_eq!(first, second);
        let (Some(Variant::Map(first)), Some(Variant::Map(second))) = (&first, &second) else {
            panic!("extended metadata is not a map");
        };
        assert!(
            std::sync::Arc::ptr_eq(first, second),
            "repeated calls must reuse one allocation"
        );

        // A different metadata object invalidates the cache.
        let other = Some(Variant::from(ConnectorMetadata::new()));
        let extended = delete_metadata.extend(&other);
        assert_eq!(is_delete(&extended), Variant::Boolean(true));
        assert_eq!(
            extended.as_ref().unwrap().index_string("topic"),
            Variant::SqlNull
        );
    }
}
