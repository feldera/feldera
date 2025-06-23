//! Routines for 256-bit integer arithmetic.

use std::{
    fmt::Debug,
    ops::{Add, Shr, Sub},
};

use crate::pow10;

const fn lo(x: u128) -> u128 {
    x & ((1 << 64) - 1)
}
const fn hi(x: u128) -> u128 {
    x >> 64
}
const fn hi_lo(x: u128) -> (u128, u128) {
    (hi(x), lo(x))
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct U256(u128, u128);

impl U256 {
    pub const ZERO: Self = U256(0, 0);

    /// Returns `x * y`.
    ///
    /// This could use [u128::widening_mul] if that's ever stabilized.
    pub const fn from_product(x: u128, y: u128) -> Self {
        // Grade-school multiplication:
        //
        // ```ignore
        //        a  b
        //    ×   c  d
        // -----------
        //       ad bd
        // +  ac bc
        // -----------
        //       |---| sum0
        // |---| sum1
        // ```

        let (a, b) = hi_lo(x);
        let (c, d) = hi_lo(y);

        // These can't overflow and `wrapping_mul` doesn't waste time checking.
        let ad = a.wrapping_mul(d);
        let bd = b.wrapping_mul(d);
        let ac = a.wrapping_mul(c);
        let bc = b.wrapping_mul(c);

        // Calculate lower half of result and carry-outs.
        let (sum0, carry0) = bd.overflowing_add(ad << 64);
        let (sum0, carry1) = sum0.overflowing_add(bc << 64);

        // Calculate upper half of result, incorporating carry-ins.
        let sum1 = ac
            .wrapping_add(hi(ad))
            .wrapping_add(hi(bc))
            .wrapping_add(carry0 as u128)
            .wrapping_add(carry1 as u128);
        Self(sum1, sum0)
    }

    pub fn isqrt(&self) -> u128 {
        if self.0 == 0 {
            self.1.isqrt()
        } else {
            let small_candidate = (self >> 2).isqrt() << 1;
            let large_candidate = small_candidate + 1;
            let product = U256::from_product(large_candidate, large_candidate);
            if product > *self {
                small_candidate
            } else {
                large_candidate
            }
        }
    }

    /// Divides by `v` and returns the result if it fits in `u128` or `None`
    /// otherwise.
    ///
    /// Algorithm from H. S. Warren, _Hacker's Delight, 2nd Ed._, Fig. 9-3.
    pub fn narrowing_div(self, v: u128) -> Option<u128> {
        let Self(u1, u0) = self;
        if v == 0 {
            None
        } else if u1 == 0 {
            Some(u0 / v)
        } else if u1 >= v {
            None
        } else {
            // We're doing grade-school division in base 2**64.  This is the number
            // base.
            const BASE: u128 = 1 << 64;

            // Shift `v` so that it occupies the most-significant bits.
            let s = v.leading_zeros();
            let v = v << s;

            let (vn1, vn0) = hi_lo(v);
            let un32 = (u1 << s) | u0.unbounded_shr(128 - s);
            let (un1, un0) = hi_lo(u0 << s);

            let mut q1 = un32 / vn1;
            let mut rhat = un32 - q1 * vn1;
            while q1 >= BASE || q1 * vn0 > BASE * rhat + un1 {
                q1 -= 1;
                rhat += vn1;
                if rhat >= BASE {
                    break;
                }
            }

            let un21 = un32
                .wrapping_mul(BASE)
                .wrapping_add(un1)
                .wrapping_sub(q1.wrapping_mul(v));

            let mut q0 = un21 / vn1;
            let mut rhat = un21 - q0 * vn1;
            while q0 >= BASE || q0 * vn0 > BASE * rhat + un0 {
                q0 -= 1;
                rhat += vn1;
                if rhat >= BASE {
                    break;
                }
            }

            // We don't currently need the remainder; this is how to calculate it:
            //
            // ```ignore
            // let _remainder = (un21 * BASE + un0 - q0 * v) >> s;
            // ```

            Some(q1 * BASE + q0)
        }
    }

    /// Shifts this value just enough digits right that it fits in `u128`.
    /// Returns the shifted value and the number of digits that were shifted.
    pub fn reduce_to_u128(self) -> (u128, usize) {
        if self.0 > 0 {
            let shift = self.0.ilog10() as usize + 1;
            (
                self.narrowing_div(pow10(shift).cast_unsigned()).unwrap(),
                shift,
            )
        } else {
            (self.1, 0)
        }
    }
}

impl Debug for U256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:032x}:{:032x}]", self.0, self.1)
    }
}

impl Add for U256 {
    type Output = U256;

    fn add(self, other: U256) -> Self::Output {
        let (sum0, carry) = self.1.overflowing_add(other.1);
        let sum1 = self.0.wrapping_add(other.0).wrapping_add(carry as u128);
        Self(sum1, sum0)
    }
}

impl Sub for U256 {
    type Output = U256;

    fn sub(self, other: U256) -> Self::Output {
        let (diff0, borrow) = self.1.overflowing_sub(other.1);
        let diff1 = self.0.wrapping_sub(other.0).wrapping_sub(borrow as u128);
        Self(diff1, diff0)
    }
}

impl Shr<u32> for &U256 {
    type Output = U256;

    fn shr(self, n: u32) -> Self::Output {
        U256(
            self.0.unbounded_shr(n),
            self.1.unbounded_shr(n)
                | self.0.unbounded_shl(128u32.wrapping_sub(n))
                | self.0.unbounded_shr(n.wrapping_sub(128)),
        )
    }
}

impl From<u128> for U256 {
    fn from(value: u128) -> Self {
        Self(0, value)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct I256 {
    value: U256,
    negative: bool,
}

impl I256 {
    /// Returns `x * y`.
    pub fn from_product(x: i128, y: i128) -> Self {
        Self {
            value: U256::from_product(x.unsigned_abs(), y.unsigned_abs()),
            negative: (x < 0) != (y < 0),
        }
    }

    /// Divides by `v` and returns the result if it fits in `i128` or `None`
    /// otherwise.
    pub fn narrowing_div(self, v: i128) -> Option<i128> {
        let result: i128 = self
            .value
            .narrowing_div(v.unsigned_abs())?
            .try_into()
            .ok()?;
        if self.negative != (v < 0) {
            Some(-result)
        } else {
            Some(result)
        }
    }

    pub fn is_negative(&self) -> bool {
        self.negative && self.value != U256::ZERO
    }

    pub fn checked_isqrt(self) -> Option<i128> {
        if self.is_negative() {
            None
        } else {
            Some(self.value.isqrt().try_into().ok()?)
        }
    }

    /// Shifts this value just enough gitis right that it fits in an `i128`.
    /// Returns the shifted value and the number of digits that were shifted.
    pub fn reduce_to_i128(self) -> (i128, usize) {
        // First reduce to the range of `u128`.
        let (value, shift) = self.value.reduce_to_u128();

        // Then if it's too big for `i128`, reduce one more time.
        let (value, shift) = if value <= i128::MAX.cast_unsigned() {
            (value.cast_signed(), shift)
        } else {
            ((value / 10).cast_signed(), shift + 1)
        };

        // Reattach sign.
        if self.negative {
            (-value, shift)
        } else {
            (value, shift)
        }
    }
}

impl Add for I256 {
    type Output = I256;

    fn add(self, other: Self) -> Self::Output {
        if self.negative == other.negative {
            Self {
                value: self.value + other.value,
                negative: self.negative,
            }
        } else if other.value <= self.value {
            Self {
                value: self.value - other.value,
                negative: self.negative,
            }
        } else {
            Self {
                value: other.value - self.value,
                negative: other.negative,
            }
        }
    }
}

impl From<i128> for I256 {
    fn from(value: i128) -> Self {
        Self {
            value: U256::from(value.unsigned_abs()),
            negative: value < 0,
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use num_bigint::BigUint;

    use crate::u256::U256;

    /// Iterator for 32 key values of `u64`.
    ///
    /// This iterates through all the possible 1-bit values of `a`, `b`, `c`,
    /// `d`, and `e`, producing the bit patterns shown below:
    ///
    /// ```ignore
    /// abccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccde
    /// ```
    ///
    /// This tries to exercise arithmetic tests for all the ways that carry may
    /// or may not occur (especially with `c = 1`).
    #[derive(Clone)]
    struct Values64(u64);
    impl Iterator for Values64 {
        type Item = u64;

        fn next(&mut self) -> Option<Self::Item> {
            if self.0 < (1 << 5) {
                // Bits 62,63 come from bits 3,4.
                let high = (self.0 & 0b11000) << 59;

                // Copy bit 3 into all the middle bits 2..=61.
                //
                // This ensures that we test cases that require carries.
                let mid = if (self.0 & 0b100) != 0 {
                    (u64::MAX >> 4) << 2
                } else {
                    0
                };

                // Bits 0,1 come from bits 0,1.
                let low = self.0 & 0b11;

                self.0 += 1;
                Some(low | mid | high)
            } else {
                None
            }
        }
    }

    fn u128_from_hi_lo(hi: u64, lo: u64) -> u128 {
        (u128::from(hi) << 64) | u128::from(lo)
    }

    // Iterator for 1,024 key values of u128.
    fn values128() -> impl Iterator<Item = u128> + Clone {
        Values64(0)
            .cartesian_product(Values64(0))
            .map(|(a, b)| u128_from_hi_lo(a, b))
    }

    // Iterator for 32,768 key values of U256.
    fn values256() -> impl Iterator<Item = U256> {
        Values64(0)
            .cartesian_product(Values64(0))
            .cartesian_product(Values64(0))
            .map(|((a, b), c)| U256(u128_from_hi_lo(a, b), u128_from_hi_lo(b, c)))
    }

    #[test]
    fn u256_from_product() {
        for x in values128() {
            for y in values128() {
                let product_u256 = U256::from_product(x, y);
                let product_biguint = BigUint::from(x) * BigUint::from(y);
                assert_eq!(
                    product_biguint,
                    (BigUint::from(product_u256.0) << 128) | BigUint::from(product_u256.1)
                );
            }
        }
    }

    #[test]
    fn u256_narrowing_div() {
        for u in values256() {
            for v in values128() {
                match u.narrowing_div(v) {
                    None => assert!(u.0 >= v || v == 0,),
                    Some(q) => {
                        let product = U256::from_product(q, v);
                        assert!(product <= u);
                        assert!(product + U256::from(v) > u);
                    }
                }
            }
        }
    }

    #[test]
    fn u256_isqrt() {
        for value in values256() {
            let root = value.isqrt();
            assert!(U256::from_product(root, root) <= value);
            if root < u128::MAX {
                assert!(
                    U256::from_product(root + 1, root + 1) > value,
                    "{:032x}:{:032x} => {root:032x}",
                    value.0,
                    value.1
                );
            }
        }
    }
}
