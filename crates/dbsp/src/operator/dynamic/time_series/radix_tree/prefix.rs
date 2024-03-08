use std::{
    cmp::min,
    fmt::{self, Debug, Display, Formatter},
    mem::size_of,
};

use num::PrimInt;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::{dynamic::DynDataTyped, operator::dynamic::time_series::Range, DBData};

use super::{RADIX, RADIX_BITS};

/// Describes a range of timestamps that share a common prefix.
#[derive(
    Clone,
    Debug,
    Default,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Eq, PartialEq, PartialOrd, Ord))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Prefix<TS: DBData> {
    /// Prefix bits.
    pub key: TS,
    /// Prefix length.
    pub prefix_len: u32,
}

impl<TS> Display for Prefix<TS>
where
    TS: DBData + PrimInt,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        if self.prefix_len == Self::KEY_BITS {
            write!(f, "{:x?}", self.key.to_u128().unwrap())
        } else if self.prefix_len == 0 {
            write!(f, "*")
        } else {
            write!(
                f,
                "{:x?}/{}",
                (self.key >> (Self::KEY_BITS - self.prefix_len) as usize)
                    .to_u128()
                    .unwrap(),
                self.prefix_len
            )
        }
    }
}

impl<TS> Prefix<TS>
where
    TS: DBData + PrimInt,
{
    const KEY_BITS: u32 = (size_of::<TS>() * 8) as u32;

    /// Creates a prefix of `prefix_len` with specified and
    #[cfg(test)]
    fn new(key: TS, prefix_len: u32) -> Self {
        Self { key, prefix_len }
    }

    /// Create a leaf prefix with all bits fixed.
    pub fn from_timestamp(key: TS) -> Self {
        Self {
            key,
            prefix_len: Self::KEY_BITS,
        }
    }

    /// `true` iff `self` is a leaf prefix, which covers a single fixed
    /// timestamp.
    pub fn is_leaf(&self) -> bool {
        self.prefix_len == Self::KEY_BITS
    }

    /// Create a prefix of length 0, which covers all possible timestamp values.
    pub fn full_range() -> Self {
        Self {
            key: TS::zero(),
            prefix_len: 0,
        }
    }

    /// The largest timestamps covered by `self`.
    fn upper(&self) -> TS {
        self.key | !Self::prefix_mask(self.prefix_len)
    }

    fn wildcard_bits(prefix_len: u32) -> usize {
        (Self::KEY_BITS - prefix_len) as usize
    }

    /// Returns bit mask with `prefix_len` higher-order bits set to `1`.
    fn prefix_mask(prefix_len: u32) -> TS {
        if prefix_len == 0 {
            TS::zero()
        } else {
            (TS::max_value() >> Self::wildcard_bits(prefix_len)) << Self::wildcard_bits(prefix_len)
        }
    }

    /// Computes the longest common prefix that covers both `self` and `key`.
    pub fn longest_common_prefix(&self, key: TS) -> Self {
        let longest_common_len = min((key ^ self.key).leading_zeros(), self.prefix_len);
        let prefix_len = longest_common_len - longest_common_len % RADIX_BITS;

        Self {
            key: key & Self::prefix_mask(prefix_len),
            prefix_len,
        }
    }

    /// `true` iff `self` contains `key`.
    pub fn contains(&self, key: TS) -> bool {
        //println!("contains prefix_mask: {:x?}", Self::prefix_mask(self.prefix_len));
        (self.key & Self::prefix_mask(self.prefix_len))
            == (key & Self::prefix_mask(self.prefix_len))
    }

    /// Child subtree of `self` that `key` belongs to.
    ///
    /// Precondition: `self` is not a leaf node, `self` contains `key`.
    pub fn slot_of_timestamp(&self, key: TS) -> usize {
        debug_assert!(self.prefix_len < Self::KEY_BITS);
        debug_assert!(self.contains(key));

        ((key >> Self::wildcard_bits(self.prefix_len + RADIX_BITS)) & TS::from(RADIX - 1).unwrap())
            .to_usize()
            .unwrap()
    }

    /// Child subtree of `self` that contains `other`.
    ///
    /// Precondition: `self` is a prefix of `other`.
    pub(crate) fn slot_of(&self, other: &Self) -> usize {
        debug_assert!(self.prefix_len < other.prefix_len);

        self.slot_of_timestamp(other.key)
    }

    /// Extends `self` with `RADIX_BITS` bits of `slot`.
    pub fn extend(&self, slot: usize) -> Self {
        debug_assert!(self.prefix_len < Self::KEY_BITS);
        debug_assert!(slot < RADIX);

        let prefix_len = self.prefix_len + RADIX_BITS;

        Self {
            key: self.key | (TS::from(slot).unwrap() << Self::wildcard_bits(prefix_len)),
            prefix_len,
        }
    }

    /// `true` iff `self` is completely contained within range.
    pub fn in_range(&self, range: &Range<TS>) -> bool {
        range.from <= self.key && range.to >= self.upper()
    }
}

pub type DynPrefix<TS> = DynDataTyped<Prefix<TS>>;

#[cfg(test)]
mod test {
    use rkyv::{archived_root, to_bytes, Deserialize, Infallible};

    use crate::operator::dynamic::time_series::radix_tree::{Prefix, RADIX_BITS};

    #[test]
    fn test_prefix() {
        type TestPrefix = Prefix<u64>;

        assert_eq!(TestPrefix::from_timestamp(0), Prefix::new(0, 64));
        assert_eq!(
            TestPrefix::from_timestamp(u64::MAX),
            Prefix::new(u64::MAX, 64)
        );
        assert_eq!(TestPrefix::full_range(), Prefix::new(0, 0));
        assert!(!TestPrefix::full_range().is_leaf());
        assert!(TestPrefix::from_timestamp(100).is_leaf());
        assert!(!Prefix::new(100, RADIX_BITS * 2).is_leaf());
        assert_eq!(TestPrefix::prefix_mask(0), 0);
        assert_eq!(TestPrefix::prefix_mask(4), 0xf000_0000_0000_0000);
        assert_eq!(TestPrefix::prefix_mask(32), 0xffff_ffff_0000_0000);
        assert_eq!(TestPrefix::prefix_mask(64), 0xffff_ffff_ffff_ffff);
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xffff_ffff_ffff_ffff),
            Prefix::new(0xff00_0000_0000_0000u64, 8)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xf0ff_ffff_ffff_ffff),
            Prefix::new(0xf000_0000_0000_0000u64, 4)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xfcff_ffff_ffff_ffff),
            Prefix::new(0xf000_0000_0000_0000u64, 4)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0x00ff_ffff_ffff_ffff),
            Prefix::new(0x0000_0000_0000_0000u64, 0)
        );
        assert_eq!(
            Prefix::new(0xffff_0000_ffff_0000u64, 64).longest_common_prefix(0xffff_0000_ffff_0000),
            Prefix::new(0xffff_0000_ffff_0000u64, 64)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).upper(),
            0xffff_ffff_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).upper(),
            0xffff_ffff_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0x1234_5678_0000_0000u64, 32).upper(),
            0x1234_5678_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0x1234_5678_0000_1111u64, 64).upper(),
            0x1234_5678_0000_1111u64
        );

        assert!(Prefix::new(0xffff_0000_ffff_0000u64, 64).contains(0xffff_0000_ffff_0000));
        assert!(!Prefix::new(0xffff_0000_ffff_0000u64, 64).contains(0xffff_0000_ffff_00ff));
        assert!(Prefix::new(0xffff_ffff_0000_0000u64, 32).contains(0xffff_ffff_ffff_ffff));
        assert!(!Prefix::new(0xffff_ffff_0000_0000u64, 32).contains(0xffff_0000_ffff_ffff));
        assert!(Prefix::new(0x0000_0000_0000_0000u64, 0).contains(0xffff_ffff_ffff_ffff));
        assert!(Prefix::new(0x0000_0000_0000_0000u64, 0).contains(0x0000_0000_0000_0000));

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0x0000_0000_0000_0001),
            0
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0x1000_0000_0000_0001),
            1
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0xa000_0000_0000_0001),
            0xa
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0xf000_0000_0000_0001),
            0xf
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_0000_0001),
            0
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_1000_0001),
            1
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_a000_0001),
            0xa
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_f000_0001),
            0xf
        );

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0x0000_0000_0000_0000, 4)),
            0
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0x1000_0000_0000_0000, 8)),
            1
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0xa000_0000_0000_0000, 16)),
            0xa
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0xf000_0000_0000_0000, 32)),
            0xf
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_0000_0000, 36)),
            0
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_1000_0000, 40)),
            1
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_a000_0000, 44)),
            0xa
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_f000_0000, 64)),
            0xf
        );

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0),
            Prefix::new(0x0000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(1),
            Prefix::new(0x1000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0xa),
            Prefix::new(0xa000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0xf),
            Prefix::new(0xf000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0),
            Prefix::new(0xffff_ffff_0000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(1),
            Prefix::new(0xffff_ffff_1000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0xa),
            Prefix::new(0xffff_ffff_a000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0xf),
            Prefix::new(0xffff_ffff_f000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0),
            Prefix::new(0xffff_ffff_0000_0000, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(1),
            Prefix::new(0xffff_ffff_0000_0001, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0xa),
            Prefix::new(0xffff_ffff_0000_000a, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0xf),
            Prefix::new(0xffff_ffff_0000_000f, 64)
        );
    }

    #[test]
    fn prefix_decode_encode() {
        type Type = Prefix<u64>;
        for input in [
            Prefix::new(0xffff_ffff_0000_0000u64, 32),
            Prefix::new(0x1234_5678_0000_1111u64, 64),
            Prefix::new(u64::MAX, 64),
        ] {
            let input: Type = input;
            let encoded = to_bytes::<_, 4096>(&input).unwrap();
            let archived = unsafe { archived_root::<Type>(&encoded[..]) };
            let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }
}
