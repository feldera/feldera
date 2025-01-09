//! Uuid operations

use dbsp::NumEntries;
use feldera_types::{deserialize_without_context, serialize_without_context};
use serde::Serialize;
use size_of::{Context, SizeOf};
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
    Serialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct Uuid {
    value: uuid::Uuid,
}

impl SizeOf for Uuid {
    fn size_of_children(&self, _context: &mut Context) {}
}

#[doc(hidden)]
impl NumEntries for &Uuid {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[doc(hidden)]
    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[doc(hidden)]
    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

deserialize_without_context!(Uuid);
serialize_without_context!(Uuid);

impl Uuid {
    /// Allocate Uuid from a byte array of correct endianness
    pub fn from_bytes(data: [u8; 16]) -> Self {
        Self {
            value: uuid::Uuid::from_bytes(data),
        }
    }

    /// Emit the Uuid as a byte slice
    pub fn to_bytes(&self) -> &[u8; 16] {
        self.value.as_bytes()
    }

    /// Convert Uuid to string representation
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(self) -> String {
        self.value.to_string()
    }

    /// Parse a string into a Uuid
    pub fn from_string(value: &String) -> Self {
        Self {
            value: uuid::Uuid::parse_str(value)
                .unwrap_or_else(|_| panic!("Cannot parse {value} into a UUID")),
        }
    }
}
