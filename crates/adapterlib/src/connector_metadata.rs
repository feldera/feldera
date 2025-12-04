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
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, name: &str, value: Variant) {
        self.0.insert(Variant::String(SqlString::from(name)), value);
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
