//! Tuple types for which we control trait implementations.

#![allow(non_snake_case)]

// This was introduced to resolve issues with auto-derived rkyv trait
// implementations.

use feldera_types::deserialize_without_context;

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct TupleBitmap<const N: usize> {
    bytes: [u8; N],
}

impl<const N: usize> Default for TupleBitmap<N> {
    fn default() -> Self {
        Self { bytes: [0u8; N] }
    }
}

impl<const N: usize> TupleBitmap<N> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn set_none(&mut self, idx: usize) {
        debug_assert!(idx < N * 8);
        let byte = idx / 8;
        let bit = idx % 8;
        self.bytes[byte] |= 1u8 << bit;
    }

    #[inline]
    pub fn is_none(&self, idx: usize) -> bool {
        debug_assert!(idx < N * 8);
        let byte = idx / 8;
        let bit = idx % 8;
        (self.bytes[byte] & (1u8 << bit)) != 0
    }

    #[inline]
    pub fn count_none(&self, fields: usize) -> usize {
        debug_assert!(fields <= N * 8);
        let full_bytes = fields / 8;
        let rem_bits = fields % 8;
        let mut count = 0usize;
        let mut i = 0usize;
        while i < full_bytes {
            count += self.bytes[i].count_ones() as usize;
            i += 1;
        }
        if rem_bits != 0 {
            let mask = (1u8 << rem_bits) - 1;
            count += (self.bytes[full_bytes] & mask).count_ones() as usize;
        }
        count
    }

    #[inline]
    pub fn count_none_before(&self, field_idx: usize) -> usize {
        self.count_none(field_idx)
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TupleFormat {
    Sparse = 0,
    Dense = 1,
}

// Make sure to also call `dbsp_adapters::deserialize_without_context!`
// and `sltsqlvalue::to_sql_row_impl!` for each new tuple type.
// Also the compiler currently generates Tup11..Tup* if necessary,
// so if e.g., we add Tup11 here the compiler needs to be adjusted too.
feldera_macros::declare_tuple! { Tup1<T1> }
feldera_macros::declare_tuple! { Tup2<T1, T2> }
feldera_macros::declare_tuple! { Tup3<T1, T2, T3> }
feldera_macros::declare_tuple! { Tup4<T1, T2, T3, T4> }
feldera_macros::declare_tuple! { Tup5<T1, T2, T3, T4, T5> }
feldera_macros::declare_tuple! { Tup6<T1, T2, T3, T4, T5, T6> }
feldera_macros::declare_tuple! { Tup7<T1, T2, T3, T4, T5, T6, T7> }
feldera_macros::declare_tuple! { Tup8<T1, T2, T3, T4, T5, T6, T7, T8> }
feldera_macros::declare_tuple! { Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9> }
feldera_macros::declare_tuple! { Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> }

deserialize_without_context!(Tup1, T1);
deserialize_without_context!(Tup2, T1, T2);
deserialize_without_context!(Tup3, T1, T2, T3);
deserialize_without_context!(Tup4, T1, T2, T3, T4);
deserialize_without_context!(Tup5, T1, T2, T3, T4, T5);
deserialize_without_context!(Tup6, T1, T2, T3, T4, T5, T6);
deserialize_without_context!(Tup7, T1, T2, T3, T4, T5, T6, T7);
deserialize_without_context!(Tup8, T1, T2, T3, T4, T5, T6, T7, T8);
deserialize_without_context!(Tup9, T1, T2, T3, T4, T5, T6, T7, T8, T9);
deserialize_without_context!(Tup10, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);

#[derive(
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    derive_more::Neg,
    serde::Serialize,
    serde::Deserialize,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound())]
#[archive(compare(PartialEq, PartialOrd))]
/// A unit tuple type on which `dbsp` controls trait implementations.
pub struct Tup0();

impl Tup0 {
    pub fn new() -> Self {
        Self()
    }
}

impl<W> crate::algebra::MulByRef<W> for Tup0
where
    W: crate::algebra::ZRingValue,
{
    type Output = Self;
    fn mul_by_ref(&self, _other: &W) -> Self::Output {
        Tup0()
    }
}

impl crate::algebra::HasZero for Tup0 {
    fn zero() -> Self {
        Tup0()
    }
    fn is_zero(&self) -> bool {
        true
    }
}

impl crate::algebra::AddByRef for Tup0 {
    fn add_by_ref(&self, _other: &Self) -> Self {
        Tup0()
    }
}

impl crate::NumEntries for Tup0 {
    const CONST_NUM_ENTRIES: Option<usize> = None;
    fn num_entries_shallow(&self) -> usize {
        0
    }
    fn num_entries_deep(&self) -> usize {
        0
    }
}

impl crate::algebra::AddAssignByRef for Tup0 {
    fn add_assign_by_ref(&mut self, _other: &Self) {}
}

impl crate::algebra::NegByRef for Tup0 {
    fn neg_by_ref(&self) -> Self {
        Tup0()
    }
}

impl From<()> for Tup0 {
    fn from((): ()) -> Self {
        Self()
    }
}

#[allow(clippy::from_over_into)]
impl Into<()> for Tup0 {
    fn into(self) {}
}

impl core::fmt::Debug for Tup0 {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        // For some reason Rust does not print anything when the tuple is empty
        f.debug_tuple("()").finish()
    }
}

impl Copy for Tup0 {}
