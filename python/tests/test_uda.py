import unittest

from feldera import PipelineBuilder
from tests import TEST_CLIENT


class TestUDA(unittest.TestCase):
    def test_local(self):
        sql = """
CREATE LINEAR AGGREGATE I128_SUM(s BINARY(16)) RETURNS BINARY(16);
CREATE TABLE T(x BINARY(16));
CREATE MATERIALIZED VIEW V AS SELECT I128_SUM(x) AS S, COUNT(*) AS C FROM T;
        """

        toml = """
i256 = { version = "0.2.2", features = ["num-traits"] }
num-traits = "0.2.19"
        """

        udfs = """
use i256::I256;
use feldera_sqllib::*;
use crate::{AddAssignByRef, AddByRef, HasZero, MulByRef, SizeOf, Tup3};
use derive_more::Add;
use num_traits::Zero;
use rkyv::Fallible;
use std::ops::{Add, AddAssign};

#[derive(Add, Clone, Debug, Default, PartialOrd, Ord, Eq, PartialEq, Hash)]
pub struct I256Wrapper {
    pub data: I256,
}

impl SizeOf for I256Wrapper {
    fn size_of_children(&self, context: &mut size_of::Context) {}
}

impl From<[u8; 32]> for I256Wrapper {
    fn from(value: [u8; 32]) -> Self {
        Self { data: I256::from_be_bytes(value) }
    }
}

impl From<&[u8]> for I256Wrapper {
    fn from(value: &[u8]) -> Self {
        let mut padded = [0u8; 32];
        // If original value is negative, pad with sign
        if value[0] & 0x80 != 0 {
            padded.fill(0xff);
        }
        let len = value.len();
        if len > 32 {
            panic!("Slice larger than target");
        }
        padded[32-len..].copy_from_slice(&value[..len]);
        Self { data: I256::from_be_bytes(padded) }
    }
}

impl MulByRef<Weight> for I256Wrapper {
    type Output = Self;

    fn mul_by_ref(&self, other: &Weight) -> Self::Output {
        println!("Mul {:?} by {}", self, other);
        Self {
            data: self.data.checked_mul_i64(*other)
                .expect("Overflow during multiplication"),
        }
    }
}

impl HasZero for I256Wrapper {
    fn zero() -> Self {
        Self { data: I256::zero() }
    }

    fn is_zero(&self) -> bool {
        self.data.is_zero()
    }
}

impl AddByRef for I256Wrapper {
    fn add_by_ref(&self, other: &Self) -> Self {
        Self { data: self.data.add(other.data) }
    }
}

impl AddAssignByRef<Self> for I256Wrapper {
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.data += other.data
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct ArchivedI256Wrapper {
    pub bytes: [u8; 32],
}

impl rkyv::Archive for I256Wrapper {
    type Archived = ArchivedI256Wrapper;
    type Resolver = ();

    #[inline]
    unsafe fn resolve(&self, pos: usize, _: Self::Resolver, out: *mut Self::Archived) {
        out.write(ArchivedI256Wrapper {
            bytes: self.data.to_be_bytes(),
        });
    }
}

impl<S: Fallible + ?Sized> rkyv::Serialize<S> for I256Wrapper {
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(())
    }
}

impl<D: Fallible + ?Sized> rkyv::Deserialize<I256Wrapper, D> for ArchivedI256Wrapper {
    #[inline]
    fn deserialize(&self, _: &mut D) -> Result<I256Wrapper, D::Error> {
        Ok(I256Wrapper::from(self.bytes))
    }
}

pub type i128_sum_accumulator_type = Tup3<I256Wrapper, i64, i64>;

pub fn i128_sum_map(val: Option<ByteArray>) -> i128_sum_accumulator_type {
    match val {
        None => Tup3::new(I256Wrapper::zero(), 0, 1),
        Some(val) => Tup3::new(
           I256Wrapper::from(val.as_slice()),
           1,
           1,
        ),
    }
}

pub fn i128_sum_post(val: i128_sum_accumulator_type) -> Option<ByteArray> {
    if val.1 == 0 {
       None
    } else {
       // Check for overflow
       if val.0.data < I256::from(i128::MIN) || val.0.data > I256::from(i128::MAX) {
           panic!("Result of aggregation {} does not fit in 128 bits", val.0.data);
       }
       Some(ByteArray::new(&val.0.data.to_be_bytes()[16..]))
    }
}
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_uda", sql=sql, udf_rust=udfs, udf_toml=toml
        ).create_or_replace()

        pipeline.start()
        pipeline.input_json(
            "t",
            [
                {
                    "insert": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    }
                }
            ],
            update_format="insert_delete",
        )
        pipeline.wait_for_idle()
        output = list(pipeline.query("SELECT * FROM V;"))
        assert output == [{"s": "00000000000000000000000000000001", "c": 1}]

        # Insert -1
        pipeline.input_json(
            "t",
            [
                {
                    "insert": {
                        "x": [
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                        ],
                    }
                }
            ],
            update_format="insert_delete",
        )
        pipeline.wait_for_idle()
        output = list(pipeline.query("SELECT * FROM V;"))
        assert output == [{"s": "00000000000000000000000000000000", "c": 2}]

        pipeline.input_json(
            "t",
            [
                {
                    "insert": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                    }
                }
            ],
            update_format="insert_delete",
        )
        output = list(pipeline.query("SELECT * FROM V;"))
        assert output == [{"s": "00000000000000000000000000000002", "c": 3}]

        pipeline.input_json(
            "t",
            [
                {
                    "insert": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
                    }
                }
            ],
            update_format="insert_delete",
        )
        output = list(pipeline.query("SELECT * FROM V;"))
        assert output == [{"s": "00000000000000000000000000000005", "c": 4}]

        pipeline.input_json(
            "t",
            [
                {
                    "delete": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    }
                },
                {
                    "delete": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                    }
                },
                {
                    "delete": {
                        "x": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
                    }
                },
                {
                    "delete": {
                        "x": [
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            255,
                            1,
                        ],
                    }
                },
            ],
            update_format="insert_delete",
        )
        output = list(pipeline.query("SELECT * FROM V;"))
        assert output == [{"s": None, "c": 0}]

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
