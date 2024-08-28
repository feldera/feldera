//! Support for byte arrays (binary objects in SQL)

use crate::{some_function1, some_function2, some_function3, some_function4};
use dbsp::num_entries_scalar;
use flate2::read::GzDecoder;
use hex::ToHex;
use pipeline_types::{deserialize_without_context, serialize_without_context};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::{cmp::min, fmt::Debug, io::Read};

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
    Deserialize,
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

serialize_without_context!(ByteArray);
deserialize_without_context!(ByteArray);

num_entries_scalar! {
    // TODO: Is this right?
    ByteArray,
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

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

pub fn to_hex_(value: ByteArray) -> String {
    value.data.encode_hex::<String>()
}

pub fn concat_bytes_bytes(left: ByteArray, right: ByteArray) -> ByteArray {
    left.concat(&right)
}

some_function1!(to_hex, ByteArray, String);

pub fn octet_length_(value: ByteArray) -> i32 {
    value.length() as i32
}

some_function1!(octet_length, ByteArray, i32);

pub fn position__(needle: ByteArray, haystack: ByteArray) -> i32 {
    haystack
        .data
        .windows(needle.data.len())
        .position(|window| window == needle.data)
        .map(|v| v + 1)
        .unwrap_or(0) as i32
}

some_function2!(position, ByteArray, ByteArray, i32);

pub fn substring2__(source: ByteArray, left: i32) -> ByteArray {
    // SQL indexing starts at 1
    let start = if left < 1 { 0 } else { left - 1 };
    let data = source.data.into_iter().skip(start as usize).collect();

    ByteArray { data }
}

some_function2!(substring2, ByteArray, i32, ByteArray);

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

pub fn overlay3___(source: ByteArray, replacement: ByteArray, position: i32) -> ByteArray {
    let len = replacement.length() as i32;
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, ByteArray, ByteArray, i32, ByteArray);

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

pub fn gunzip_(source: ByteArray) -> String {
    let mut gz = GzDecoder::new(&source.data[..]);
    let mut s = String::new();

    gz.read_to_string(&mut s)
        .expect("failed to decompress gzipped data");
    s
}

some_function1!(gunzip, ByteArray, String);

pub fn to_int_(source: ByteArray) -> i32 {
    let mut result = 0;
    for i in 0..min(4, source.length()) {
        result = (result << 8) | (source.data[i] as i32);
    }
    result
}

some_function1!(to_int, ByteArray, i32);
