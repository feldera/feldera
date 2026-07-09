use std::{collections::BTreeMap, sync::Arc};

use feldera_sqllib::{SqlString, Variant};

/// Connector metadata attached to each input record.
///
/// Both the transport connector and the parser can add metadata attributes
/// such as Kafka topic name or Avro schema id. These attributes are passed
/// to the deserializer along with the actual record, which can use them
/// to populate some of the table columns.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct ConnectorMetadata(BTreeMap<Variant, Variant>);

impl ConnectorMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: &str, value: Variant) {
        self.0.insert(Variant::String(SqlString::from(name)), value);
    }

    /// Returns the attribute stored under `key`, or `None` if absent.
    pub fn get(&self, key: &Variant) -> Option<&Variant> {
        self.0.get(key)
    }

    /// Returns the attribute stored under the string key `name`, or `None` if
    /// absent.  Attributes added with [`insert`](Self::insert) are keyed by
    /// string.
    pub fn get_by_name(&self, name: &str) -> Option<&Variant> {
        self.0.get(&Variant::String(SqlString::from(name)))
    }
}

impl From<BTreeMap<Variant, Variant>> for ConnectorMetadata {
    fn from(metadata: BTreeMap<Variant, Variant>) -> Self {
        Self(metadata)
    }
}

impl From<ConnectorMetadata> for Variant {
    fn from(metadata: ConnectorMetadata) -> Self {
        Variant::Map(Arc::new(metadata.0))
    }
}

impl From<&ConnectorMetadata> for Variant {
    fn from(metadata: &ConnectorMetadata) -> Self {
        Variant::Map(Arc::new(metadata.0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both getters must use the same key encoding as `insert`.
    #[test]
    fn test_get_uses_insert_key_encoding() {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("topic", Variant::String(SqlString::from("events")));

        let expected = Variant::String(SqlString::from("events"));
        assert_eq!(metadata.get_by_name("topic"), Some(&expected));
        assert_eq!(
            metadata.get(&Variant::String(SqlString::from("topic"))),
            Some(&expected)
        );
        assert_eq!(metadata.get_by_name("missing"), None);
    }
}
