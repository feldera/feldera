// The record deserializer still reads the metadata as the enum `Variant`.
#![allow(deprecated)]

use std::collections::BTreeMap;

use feldera_sqllib::{SqlString, Variant};
use feldera_types::variant::VariantValue;

/// Connector metadata attached to each input record.
///
/// Both the transport connector and the parser can add metadata attributes
/// such as Kafka topic name or Avro schema id. These attributes are passed
/// to the deserializer along with the actual record, which can use them
/// to populate some of the table columns.
///
/// An attribute is a [`VariantValue`], which any runtime can read. The record
/// deserializer reads the attributes as a [`Variant`] map, one entry per
/// attribute name.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct ConnectorMetadata(BTreeMap<String, VariantValue>);

impl ConnectorMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the attribute `name`, replacing any attribute already stored
    /// under that name.
    pub fn insert(&mut self, name: &str, value: VariantValue) {
        self.0.insert(name.to_string(), value);
    }

    /// Returns the attribute stored under `name`, or `None` if absent.
    pub fn get_by_name(&self, name: &str) -> Option<&VariantValue> {
        self.0.get(name)
    }

    /// Returns `true` if no attribute has been added.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<BTreeMap<String, VariantValue>> for ConnectorMetadata {
    fn from(metadata: BTreeMap<String, VariantValue>) -> Self {
        Self(metadata)
    }
}

impl From<ConnectorMetadata> for VariantValue {
    fn from(metadata: ConnectorMetadata) -> Self {
        VariantValue::Object(metadata.0)
    }
}

impl From<ConnectorMetadata> for Variant {
    fn from(metadata: ConnectorMetadata) -> Self {
        Variant::from(&metadata)
    }
}

impl From<&ConnectorMetadata> for Variant {
    fn from(metadata: &ConnectorMetadata) -> Self {
        Variant::Map(
            metadata
                .0
                .iter()
                .map(|(name, value)| (Variant::String(SqlString::from_ref(name)), value.into()))
                .collect::<BTreeMap<_, _>>()
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An attribute reads back under the name it was added with.
    #[test]
    fn test_get_by_name_returns_inserted_attribute() {
        let mut metadata = ConnectorMetadata::new();
        assert!(metadata.is_empty());
        metadata.insert("topic", VariantValue::String("events".into()));

        assert_eq!(
            metadata.get_by_name("topic"),
            Some(&VariantValue::String("events".into()))
        );
        assert_eq!(metadata.get_by_name("missing"), None);
        assert!(!metadata.is_empty());
    }

    /// The deserializer sees a `Variant` map keyed by attribute name, each
    /// attribute converted the way any `VariantValue` converts.
    #[test]
    fn test_metadata_reads_as_a_variant_map() {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_offset", VariantValue::Int64(17));
        metadata.insert(
            "kafka_headers",
            VariantValue::Object(BTreeMap::from([
                ("trace".to_string(), VariantValue::Binary(vec![1, 2])),
                ("empty".to_string(), VariantValue::Null),
            ])),
        );

        let variant = Variant::from(&metadata);
        assert_eq!(variant.index_string("kafka_offset"), Variant::BigInt(17));
        assert_eq!(
            variant.index_string("kafka_headers").index_string("trace"),
            Variant::from(&VariantValue::Binary(vec![1, 2]))
        );
        assert_eq!(
            variant.index_string("kafka_headers").index_string("empty"),
            Variant::VariantNull
        );
        assert_eq!(Variant::from(metadata.clone()), variant);
        assert_eq!(Variant::from(&VariantValue::from(metadata)), variant);
    }

    /// A Kafka header without a value reads as NULL through the cast the Kafka
    /// connector documentation shows:
    /// `CAST(CONNECTOR_METADATA()['kafka_headers'] AS MAP<VARCHAR, VARBINARY>)`.
    #[test]
    fn test_header_without_a_value_casts_to_null() {
        use feldera_sqllib::{ByteArray, cast_to_mapN_V};

        let mut metadata = ConnectorMetadata::new();
        metadata.insert(
            "kafka_headers",
            VariantValue::Object(BTreeMap::from([
                ("trace".to_string(), VariantValue::Binary(vec![1, 2])),
                ("empty".to_string(), VariantValue::Null),
            ])),
        );

        let headers = Variant::from(&metadata).index_string("kafka_headers");
        let headers = cast_to_mapN_V::<SqlString, Option<ByteArray>>(headers)
            .unwrap()
            .expect("the headers cast to a map");
        assert_eq!(headers.get(&SqlString::from_ref("empty")), Some(&None));
        assert_eq!(
            headers.get(&SqlString::from_ref("trace")),
            Some(&Some(ByteArray::new(&[1, 2])))
        );
    }
}
