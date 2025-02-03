//! byte arrays (binary objects in SQL)

use crate::{some_function1, some_function2, some_function3, some_function4, SqlString};
use base64::prelude::*;
use dbsp::NumEntries;
use feldera_types::serde_with_context::{
    serde_config::BinaryFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use flate2::read::GzDecoder;
use hex::ToHex;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    fmt::Debug,
    io::Read,
};

/// A ByteArray object, representing a SQL value with type
/// `BINARY` or `VARBINARY`.
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
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[serde(transparent)]
pub struct ByteArray {
    data: Vec<u8>,
}

impl SerializeWithContext<SqlSerdeConfig> for ByteArray {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.binary_format {
            BinaryFormat::Array => self.data.serialize(serializer),
            BinaryFormat::Base64 => serializer.serialize_str(&BASE64_STANDARD.encode(&self.data)),
        }
    }
}

impl<'de> DeserializeWithContext<'de, SqlSerdeConfig> for ByteArray {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match config.binary_format {
            BinaryFormat::Array => {
                let data = Vec::<u8>::deserialize(deserializer)?;
                Ok(Self { data })
            }
            BinaryFormat::Base64 => {
                let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
                let data = BASE64_STANDARD
                    .decode(&*str)
                    .map_err(|e| D::Error::custom(format!("invalid base64 string: {e}")))?;
                Ok(Self { data })
            }
        }
    }
}

#[doc(hidden)]
impl NumEntries for &ByteArray {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[doc(hidden)]
    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.length()
    }

    #[doc(hidden)]
    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.length()
    }
}

impl ByteArray {
    /// Create a ByteArray from a slice of bytes
    pub fn new(d: &[u8]) -> Self {
        Self { data: d.to_vec() }
    }

    pub fn with_size(d: &[u8], size: i32) -> Self {
        if size < 0 {
            ByteArray::new(d)
        } else {
            let size = size as usize;
            match d.len().cmp(&size) {
                Ordering::Equal => ByteArray::new(d),
                Ordering::Greater => ByteArray::new(&d[..size]),
                Ordering::Less => {
                    let mut data: Vec<u8> = vec![0; size];
                    data[..d.len()].copy_from_slice(d);
                    ByteArray { data }
                }
            }
        }
    }

    pub fn zero(size: usize) -> Self {
        Self {
            data: vec![0; size],
        }
    }

    /// Create a ByteArray from a Vector of bytes
    pub fn from_vec(d: Vec<u8>) -> Self {
        Self { data: d }
    }

    /// Length of the byte array in bytes
    pub fn length(&self) -> usize {
        self.data.len()
    }

    #[doc(hidden)]
    /// Combine two byte arrays of the same length using
    /// a pointwise function.  panics if the lengths
    /// are not the same.
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

    #[doc(hidden)]
    /// Bytewise 'and' of two byte arrays of the same length.
    /// Panics if the arrays do not have the same length.
    pub fn and(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left & right)
    }

    #[doc(hidden)]
    /// Bytewise 'or' of two byte arrays of the same length.
    /// Panics if the arrays do not have the same length.
    pub fn or(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left | right)
    }

    #[doc(hidden)]
    /// Bytewise 'xor' of two byte arrays of the same length.
    /// Panics if the arrays do not have the same length.
    pub fn xor(&self, other: &Self) -> Self {
        self.zip(other, |left, right| left ^ right)
    }

    /// Concatenate two byte arrays, produces a new byte array.
    pub fn concat(&self, other: &Self) -> Self {
        let mut r = Vec::<u8>::with_capacity(self.data.len() + other.data.len());
        r.extend(&self.data);
        r.extend(&other.data);
        ByteArray::from_vec(r)
    }

    /// Get a reference to the data in a ByteArray as a byte slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[doc(hidden)]
pub fn to_hex_(value: ByteArray) -> SqlString {
    SqlString::from(value.data.encode_hex::<String>())
}

#[doc(hidden)]
pub fn concat_bytes_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.concat(&right)
}

some_function1!(to_hex, ByteArray, SqlString);

#[doc(hidden)]
pub fn octet_length_(value: ByteArray) -> i32 {
    value.length() as i32
}

some_function1!(octet_length, ByteArray, i32);

#[doc(hidden)]
pub fn position__(needle: ByteArray, haystack: ByteArray) -> i32 {
    haystack
        .data
        .windows(needle.data.len())
        .position(|window| window == needle.data)
        .map(|v| v + 1)
        .unwrap_or(0) as i32
}

some_function2!(position, ByteArray, ByteArray, i32);

#[doc(hidden)]
pub fn substring2__(source: ByteArray, left: i32) -> ByteArray {
    // SQL indexing starts at 1
    let start = if left < 1 { 0 } else { left - 1 };
    let data = source.data.into_iter().skip(start as usize).collect();

    ByteArray { data }
}

some_function2!(substring2, ByteArray, i32, ByteArray);

#[doc(hidden)]
pub fn substring3___(source: ByteArray, left: i32, count: i32) -> ByteArray {
    // SQL indexing starts at 1
    let start = if left < 1 { 0 } else { left - 1 };
    let count = usize::try_from(count).expect("negative substring length not allowed");

    let data = source
        .data
        .into_iter()
        .skip(start as usize)
        .take(count)
        .collect();

    ByteArray { data }
}

some_function3!(substring3, ByteArray, i32, i32, ByteArray);

#[doc(hidden)]
pub fn overlay3___(source: ByteArray, replacement: ByteArray, position: i32) -> ByteArray {
    let len = replacement.length() as i32;
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, ByteArray, ByteArray, i32, ByteArray);

#[doc(hidden)]
pub fn overlay4____(
    source: ByteArray,
    mut replacement: ByteArray,
    position: i32,
    remove: i32,
) -> ByteArray {
    let mut remove = remove;
    if remove < 0 {
        remove = 0;
    }

    if position <= 0 {
        source
    } else if position > source.length() as i32 {
        source.concat(&replacement)
    } else {
        let mut result = substring3___(source.clone(), 0, position - 1);
        result.data.append(&mut replacement.data);
        let mut substr = substring2__(source, position + remove);
        result.data.append(&mut substr.data);
        result
    }
}

some_function4!(overlay4, ByteArray, ByteArray, i32, i32, ByteArray);

#[doc(hidden)]
pub fn gunzip_(source: ByteArray) -> SqlString {
    let mut gz = GzDecoder::new(&source.data[..]);
    let mut s = String::new();

    gz.read_to_string(&mut s)
        .expect("failed to decompress gzipped data");
    SqlString::from(s)
}

some_function1!(gunzip, ByteArray, SqlString);

#[doc(hidden)]
pub fn to_int_(source: ByteArray) -> i32 {
    let mut result = 0;
    for i in 0..min(4, source.length()) {
        result = (result << 8) | (source.data[i] as i32);
    }
    result
}

some_function1!(to_int, ByteArray, i32);
