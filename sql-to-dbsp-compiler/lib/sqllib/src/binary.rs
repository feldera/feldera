//! Support for byte arrays (binary objects in SQL)

use dbsp_adapters::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig};
use serde::{Deserializer, Serializer};
use size_of::SizeOf;
use std::fmt::Debug;

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ByteArray {
    data: Vec<u8>,
}

impl SerializeWithContext<SqlSerdeConfig> for ByteArray {
    fn serialize_with_context<S>(
        &self,
        _serializer: S,
        _context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!();
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for ByteArray {
    fn deserialize_with_context<D>(
        _deserializer: D,
        _config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!();
    }
}

impl ByteArray {
    pub fn new(d: &[u8]) -> Self {
        Self { data: d.to_vec() }
    }
}
