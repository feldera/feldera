//! Support for byte arrays (binary objects in SQL)

use crate::some_function1;
use dbsp_adapters::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig};
use hex::ToHex;
use serde::{Deserializer, Serializer};
use size_of::SizeOf;
use std::fmt::Debug;
use dbsp::num_entries_scalar;

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

num_entries_scalar! {
    // TODO: Is this right?
    ByteArray,
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

    pub fn from_vec(d: Vec<u8>) -> Self {
        Self { data: d }
    }

    pub fn length(&self) -> usize {
        self.data.len()
    }

    pub fn zip<F>(&self, other: &Self, op: F) -> ByteArray
    where
        F: Fn(&u8, &u8) -> u8,
    {
        let self_len = self.data.len();
        let other_len = other.data.len();
        if self_len != other_len {
            panic!(
                "Cannot operate on BINARY objects of different sizes {} and {}",
                self_len, other_len
            );
        }
        let result: Vec<u8> = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(l, r)| op(l, r))
            .collect();
        ByteArray::new(&result)
    }

    pub fn and(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left & right)
    }

    pub fn or(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left | right)
    }

    pub fn xor(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left ^ right)
    }

    pub fn concat(&self, other: &Self) -> Self {
        let mut r = Vec::<u8>::with_capacity(self.data.len() + other.data.len());
        r.extend(&self.data);
        r.extend(&other.data);
        ByteArray::from_vec(r)
    }
}

pub fn to_hex_(value: ByteArray) -> String {
    value.data.encode_hex::<String>()
}

pub fn concat_bytes_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.concat(&right)
}

some_function1!(to_hex, ByteArray, String);
