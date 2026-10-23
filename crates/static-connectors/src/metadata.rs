//! Connector metadata in the form that the generated record deserializers read: a `SqlVariant`
//! object with one field per attribute.

use feldera_adapterlib::ConnectorMetadata;
use feldera_sqllib::SqlVariant;

/// `metadata` as a `SqlVariant` object, one field per attribute.
pub(crate) fn metadata_variant(metadata: &ConnectorMetadata) -> SqlVariant {
    SqlVariant::from_object(metadata.attributes())
}

/// The `SqlVariant` that a stream's record deserializer reads, encoded once per metadata value
/// rather than once per record: a parser attaches the same metadata to every record of a
/// message.
#[derive(Clone, Default)]
pub(crate) struct MetadataVariant {
    /// The metadata that `variant` was encoded from. Holding it keeps its allocation alive,
    /// which keeps the pointer comparison in [`Self::get`] meaningful.
    source: Option<ConnectorMetadata>,
    variant: Option<SqlVariant>,
}

impl MetadataVariant {
    /// `metadata` as a `SqlVariant`, encoded only if it is not the metadata that the last call
    /// encoded.
    pub(crate) fn get(&mut self, metadata: &Option<ConnectorMetadata>) -> &Option<SqlVariant> {
        let unchanged = match (&self.source, metadata) {
            (None, None) => true,
            (Some(source), Some(metadata)) => source.ptr_eq(metadata),
            _ => false,
        };
        if !unchanged {
            self.variant = metadata.as_ref().map(metadata_variant);
            self.source = metadata.clone();
        }
        &self.variant
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use feldera_adapterlib::ConnectorMetadata;
    use feldera_sqllib::{ByteArray, SqlString, SqlVariant, cast_to_mapN_FVN, indexFVN_};
    use feldera_types::variant::VariantValue;

    use super::{MetadataVariant, metadata_variant};

    fn kafka_metadata() -> ConnectorMetadata {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_offset", VariantValue::Int64(17));
        metadata.insert(
            "kafka_headers",
            VariantValue::Object(BTreeMap::from([
                ("trace".to_string(), VariantValue::Binary(vec![1, 2])),
                ("empty".to_string(), VariantValue::Null),
            ])),
        );
        metadata
    }

    /// The deserializer sees a `SqlVariant` object keyed by attribute name, each attribute
    /// encoded the way any `VariantValue` encodes.
    #[test]
    fn metadata_reads_as_a_variant_object() {
        let variant = Some(metadata_variant(&kafka_metadata()));
        let attribute = |name: &str| indexFVN_(&variant, SqlString::from_ref(name));
        assert_eq!(
            attribute("kafka_offset"),
            Some(SqlVariant::from(&VariantValue::Int64(17)))
        );
        let header = |name: &str| indexFVN_(&attribute("kafka_headers"), SqlString::from_ref(name));
        assert_eq!(
            header("trace"),
            Some(SqlVariant::from(&VariantValue::Binary(vec![1, 2])))
        );
        assert_eq!(header("empty"), Some(SqlVariant::variant_null()));
    }

    /// A Kafka header without a value reads as NULL through the cast the Kafka connector
    /// documentation shows: `CAST(CONNECTOR_METADATA()['kafka_headers'] AS MAP<VARCHAR,
    /// VARBINARY>)`.
    #[test]
    fn a_header_without_a_value_casts_to_null() {
        let headers = indexFVN_(
            &Some(metadata_variant(&kafka_metadata())),
            SqlString::from_ref("kafka_headers"),
        );
        let headers = cast_to_mapN_FVN::<SqlString, Option<ByteArray>>(headers)
            .unwrap()
            .expect("the headers cast to a map");
        assert_eq!(headers.get(&SqlString::from_ref("empty")), Some(&None));
        assert_eq!(
            headers.get(&SqlString::from_ref("trace")),
            Some(&Some(ByteArray::new(&[1, 2])))
        );
    }

    /// The cache encodes a metadata value once, also when it gets a clone of it, and again
    /// for a different value, even an equal one.
    #[test]
    fn metadata_encodes_once_per_value() {
        fn object(variant: &Option<SqlVariant>) -> SqlVariant {
            variant.clone().expect("metadata encodes to an object")
        }

        let mut cache = MetadataVariant::default();
        assert_eq!(cache.get(&None), &None);

        let metadata = Some(kafka_metadata());
        let first = object(cache.get(&metadata));
        assert!(first.ptr_eq(&object(cache.get(&metadata))));
        assert!(first.ptr_eq(&object(cache.get(&metadata.clone()))));

        let equal = object(cache.get(&Some(kafka_metadata())));
        assert_eq!(equal, first);
        assert!(!equal.ptr_eq(&first));

        assert_eq!(cache.get(&None), &None);
    }
}
