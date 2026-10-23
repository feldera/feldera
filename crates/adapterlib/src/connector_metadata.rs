// The record deserializer still reads the metadata as the enum `Variant`.
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::sync::Arc;

use feldera_types::variant::VariantValue;

/// Connector metadata attached to each input record.
///
/// Both the transport connector and the parser can add metadata attributes
/// such as Kafka topic name or Avro schema id. These attributes are passed
/// to the deserializer along with the actual record, which can use them
/// to populate some of the table columns.
///
/// An attribute is a [`VariantValue`], which any runtime can read. A clone
/// shares the attributes until one of the two changes, so a record
/// deserializer can tell by [`Self::ptr_eq`] that it already converted them.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct ConnectorMetadata(Arc<BTreeMap<String, VariantValue>>);

impl ConnectorMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the attribute `name`, replacing any attribute already stored
    /// under that name.
    pub fn insert(&mut self, name: &str, value: VariantValue) {
        Arc::make_mut(&mut self.0).insert(name.to_string(), value);
    }

    /// Returns the attribute stored under `name`, or `None` if absent.
    pub fn get_by_name(&self, name: &str) -> Option<&VariantValue> {
        self.0.get(name)
    }

    /// Returns `true` if no attribute has been added.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the attributes in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &VariantValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    /// Returns `true` if `self` and `other` are the same value, not only equal
    /// ones: one is a clone of the other, and neither changed since.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl From<BTreeMap<String, VariantValue>> for ConnectorMetadata {
    fn from(metadata: BTreeMap<String, VariantValue>) -> Self {
        Self(Arc::new(metadata))
    }
}

impl From<ConnectorMetadata> for VariantValue {
    fn from(metadata: ConnectorMetadata) -> Self {
        VariantValue::Object(Arc::unwrap_or_clone(metadata.0))
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
        assert_eq!(
            VariantValue::from(metadata),
            VariantValue::Object(BTreeMap::from([(
                "topic".to_string(),
                VariantValue::String("events".into())
            )]))
        );
    }

    /// A clone is the same value until one of the two changes, and the change
    /// leaves the other as it was.
    #[test]
    fn test_a_clone_is_the_same_value_until_it_changes() {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_offset", VariantValue::Int64(17));
        let mut clone = metadata.clone();
        assert!(clone.ptr_eq(&metadata));

        clone.insert("kafka_partition", VariantValue::Int32(3));
        assert!(!clone.ptr_eq(&metadata));
        assert_eq!(metadata.get_by_name("kafka_partition"), None);
        assert_eq!(
            clone.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["kafka_offset", "kafka_partition"]
        );

        // Equal attributes built apart are equal, but not the same value.
        let mut other = ConnectorMetadata::new();
        other.insert("kafka_offset", VariantValue::Int64(17));
        assert_eq!(other, metadata);
        assert!(!other.ptr_eq(&metadata));
    }
}
