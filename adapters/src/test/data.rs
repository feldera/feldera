use bincode::{Decode, Encode};
use proptest::{collection, prelude::*};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use size_of::SizeOf;

#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Clone,
    Hash,
    SizeOf,
    Encode,
    Decode,
    Arbitrary,
)]
pub struct TestStruct {
    pub id: u32,
    pub b: bool,
    pub i: Option<i64>,
    pub s: String,
}

pub fn generate_test_data(size: usize) -> impl Strategy<Value = Vec<TestStruct>> {
    collection::vec(any::<TestStruct>(), 0..=size)
}
