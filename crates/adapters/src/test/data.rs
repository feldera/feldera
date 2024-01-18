use proptest::{collection, prelude::*};
use proptest_derive::Arbitrary;
use size_of::SizeOf;

use crate::{deserialize_without_context, serialize_struct};

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    Arbitrary,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct TestStruct {
    pub id: u32,
    pub b: bool,
    pub i: Option<i64>,
    pub s: String,
}

deserialize_without_context!(TestStruct);

serialize_struct!(TestStruct()[4]{
    id["id"]: u32,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
});

/// Generate a batch of records no larger that `size`.
///
/// Makes sure all elements in the vector are unique and ordered.
/// This guarantees that the number and order of values remains the same
/// when they are assembled in a Z-set, which simplifies testing.
pub fn generate_test_batch(size: usize) -> impl Strategy<Value = Vec<TestStruct>> {
    collection::vec(any::<TestStruct>(), 0..=size).prop_map(|v| {
        v.into_iter()
            .enumerate()
            .map(|(i, mut val)| {
                val.id = i as u32;
                val
            })
            .collect::<Vec<_>>()
    })
}

/// Generate up to `max_batches` batches, up to `max_records` each.
///
/// Makes sure elements are unique and ordered across all batches.
pub fn generate_test_batches(
    min_batches: usize,
    max_batches: usize,
    max_records: usize,
) -> impl Strategy<Value = Vec<Vec<TestStruct>>> {
    collection::vec(
        collection::vec(any::<TestStruct>(), 0..=max_records),
        min_batches..=max_batches,
    )
    .prop_map(|batches| {
        let mut index = 0;
        batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|mut val| {
                        val.id = index;
                        index += 1;
                        val
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    })
}

pub fn generate_test_batches_with_weights(
    max_batches: usize,
    max_records: usize,
) -> impl Strategy<Value = Vec<Vec<(TestStruct, i64)>>> {
    collection::vec(
        collection::vec((any::<TestStruct>(), -2i64..=2i64), 0..=max_records),
        0..=max_batches,
    )
    .prop_map(|batches| {
        let mut index = 0;
        batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|mut val| {
                        val.0.id = index;
                        index += 1;
                        val
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    })
}
