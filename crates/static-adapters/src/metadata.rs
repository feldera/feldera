//! Connector metadata in the form that the generated record deserializers read: a `Variant`
//! map from attribute name to value.

use std::collections::BTreeMap;

use feldera_adapterlib::ConnectorMetadata;
use feldera_sqllib::{SqlString, Variant};

/// `metadata` as a `Variant` map, one entry per attribute.
pub(crate) fn metadata_variant(metadata: &ConnectorMetadata) -> Variant {
    Variant::Map(
        metadata
            .iter()
            .map(|(name, value)| (Variant::String(SqlString::from_ref(name)), value.into()))
            .collect::<BTreeMap<_, _>>()
            .into(),
    )
}

/// The `Variant` that a stream's record deserializer reads, converted once per metadata value
/// rather than once per record: a parser attaches the same metadata to every record of a
/// message.
#[derive(Clone, Default)]
pub(crate) struct MetadataVariant {
    /// The metadata that `variant` was converted from. Holding it keeps its allocation alive,
    /// which keeps the pointer comparison in [`Self::get`] meaningful.
    source: Option<ConnectorMetadata>,
    variant: Option<Variant>,
}

impl MetadataVariant {
    /// `metadata` as a `Variant`, converted only if it is not the metadata that the last call
    /// converted.
    pub(crate) fn get(&mut self, metadata: &Option<ConnectorMetadata>) -> &Option<Variant> {
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
    use std::sync::Arc;

    use feldera_adapterlib::ConnectorMetadata;
    use feldera_sqllib::{ByteArray, SqlString, Variant, cast_to_mapN_V};
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

    /// The deserializer sees a `Variant` map keyed by attribute name, each attribute converted
    /// the way any `VariantValue` converts.
    #[test]
    fn metadata_reads_as_a_variant_map() {
        let variant = metadata_variant(&kafka_metadata());
        assert_eq!(variant.index_string("kafka_offset"), Variant::BigInt(17));
        assert_eq!(
            variant.index_string("kafka_headers").index_string("trace"),
            Variant::from(&VariantValue::Binary(vec![1, 2]))
        );
        assert_eq!(
            variant.index_string("kafka_headers").index_string("empty"),
            Variant::VariantNull
        );
    }

    /// A Kafka header without a value reads as NULL through the cast the Kafka connector
    /// documentation shows: `CAST(CONNECTOR_METADATA()['kafka_headers'] AS MAP<VARCHAR,
    /// VARBINARY>)`.
    #[test]
    fn a_header_without_a_value_casts_to_null() {
        let headers = metadata_variant(&kafka_metadata()).index_string("kafka_headers");
        let headers = cast_to_mapN_V::<SqlString, Option<ByteArray>>(headers)
            .unwrap()
            .expect("the headers cast to a map");
        assert_eq!(headers.get(&SqlString::from_ref("empty")), Some(&None));
        assert_eq!(
            headers.get(&SqlString::from_ref("trace")),
            Some(&Some(ByteArray::new(&[1, 2])))
        );
    }

    /// The cache converts a metadata value once, also when it gets a clone of it, and again
    /// for a different value, even an equal one.
    #[test]
    fn metadata_converts_once_per_value() {
        fn map(variant: &Option<Variant>) -> Arc<BTreeMap<Variant, Variant>> {
            let Some(Variant::Map(map)) = variant else {
                panic!("metadata converts to a map");
            };
            map.clone()
        }

        let mut cache = MetadataVariant::default();
        assert_eq!(cache.get(&None), &None);

        let metadata = Some(kafka_metadata());
        let first = map(cache.get(&metadata));
        assert!(Arc::ptr_eq(&first, &map(cache.get(&metadata))));
        assert!(Arc::ptr_eq(&first, &map(cache.get(&metadata.clone()))));

        let equal = map(cache.get(&Some(kafka_metadata())));
        assert_eq!(equal, first);
        assert!(!Arc::ptr_eq(&equal, &first));

        assert_eq!(cache.get(&None), &None);
    }
}
