//! Tuple types for which we control trait implementations.

#![allow(non_snake_case)]

// This was introduced to resolve issues with auto-derived rkyv trait
// implementations.

use crate::DBData;
use feldera_types::deserialize_without_context;

// Make sure to also call `dbsp_adapters::deserialize_without_context!`
// and `sltsqlvalue::to_sql_row_impl!` for each new tuple type.
// Also the compiler currently generates Tup11..Tup* if necessary,
// so if e.g., we add Tup11 here the compiler needs to be adjusted too.
feldera_macros::declare_tuple! { Tup1<T1> }

#[derive(
    Default,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(
    bound(
        archive = " T1 : rkyv :: Archive, < T1 as rkyv :: Archive > :: Archived : Ord, T2 : rkyv :: Archive, < T2 as rkyv :: Archive > :: Archived : Ord,"
    )
)]
pub struct Tup2<T1, T2>(T1, T2);
impl<T1, T2> Tup2<T1, T2> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(t0: T1, t1: T2) -> Self
    where
        T1: core::hash::Hash,
        T2: core::hash::Hash,
    {
        Self(t0, t1)
    }
}
impl<T1, T2> Tup2<T1, T2> {
    #[inline]
    pub fn get_0(&self) -> &T1 {
        &self.0
    }
    #[inline]
    pub fn get_0_mut(&mut self) -> &mut T1 {
        &mut self.0
    }
}
impl<T1, T2> Tup2<T1, T2> {
    #[inline]
    pub fn get_1(&self) -> &T2 {
        &self.1
    }
    #[inline]
    pub fn get_1_mut(&mut self) -> &mut T2 {
        &mut self.1
    }
}
impl<T1, T2, W> crate::algebra::MulByRef<W> for Tup2<T1, T2>
where
    T1: crate::algebra::MulByRef<W, Output=T1>,
    T2: crate::algebra::MulByRef<W, Output=T2>,
    W: crate::algebra::ZRingValue,
{
    type Output = Self;
    fn mul_by_ref(&self, other: &W) -> Self::Output {
        let Tup2(t0, t1) = self;
        Tup2(t0.mul_by_ref(other), t1.mul_by_ref(other))
    }
}
impl<T1, T2> crate::algebra::HasZero for Tup2<T1, T2>
where
    T1: crate::algebra::HasZero,
    T2: crate::algebra::HasZero,
{
    fn zero() -> Self {
        Tup2(T1::zero(), T2::zero())
    }
    fn is_zero(&self) -> bool {
        let mut result = true;
        let Tup2(t0, t1) = self;
        result = result && t0.is_zero();
        result = result && t1.is_zero();
        result
    }
}
impl<T1, T2> crate::algebra::AddByRef for Tup2<T1, T2>
where
    T1: crate::algebra::AddByRef,
    T2: crate::algebra::AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        let Tup2(t0, t1) = self;
        let Tup2(other_t0, other_t1) = other;
        Tup2(t0.add_by_ref(other_t0), t1.add_by_ref(other_t1))
    }
}
impl<T1, T2> crate::algebra::AddAssignByRef for Tup2<T1, T2>
where
    T1: crate::algebra::AddAssignByRef,
    T2: crate::algebra::AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        let Tup2(ref mut t0, ref mut t1) = self;
        let Tup2(ref other_t0, ref other_t1) = other;
        t0.add_assign_by_ref(other_t0);
        t1.add_assign_by_ref(other_t1);
    }
}
impl<T1, T2> crate::algebra::NegByRef for Tup2<T1, T2>
where
    T1: crate::algebra::NegByRef,
    T2: crate::algebra::NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        let Tup2(t0, t1) = self;
        Tup2(t0.neg_by_ref(), t1.neg_by_ref())
    }
}
impl<T1, T2> From<(T1, T2)> for Tup2<T1, T2> {
    fn from((t0, t1): (T1, T2)) -> Self {
        Self(t0, t1)
    }
}
impl<'a, T1, T2> Into<(&'a T1, &'a T2)> for &'a Tup2<T1, T2> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (&'a T1, &'a T2) {
        let Tup2(t0, t1) = &self;
        (t0, t1)
    }
}
impl<T1, T2> Into<(T1, T2)> for Tup2<T1, T2> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (T1, T2) {
        let Tup2(t0, t1) = self;
        (t0, t1)
    }
}
impl<T1, T2> crate::NumEntries for Tup2<T1, T2>
where
    T1: crate::NumEntries,
    T2: crate::NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;
    fn num_entries_shallow(&self) -> usize {
        2usize
    }
    fn num_entries_deep(&self) -> usize {
        let Tup2(t0, t1) = self;
        0 + (t0).num_entries_deep() + (t1).num_entries_deep()
    }
}
impl<T1: core::fmt::Debug, T2: core::fmt::Debug> core::fmt::Debug for Tup2<T1, T2> {
    fn fmt(
        &self,
        f: &mut core::fmt::Formatter,
    ) -> core::result::Result<(), core::fmt::Error> {
        let Tup2(t0, t1) = self;
        f.debug_tuple(stringify!(Tup2)).field(&t0).field(&t1).finish()
    }
}
impl<T1: Copy, T2: Copy> Copy for Tup2<T1, T2> {}
impl<T1, T2> crate::circuit::checkpointer::Checkpoint for Tup2<T1, T2>
where
    Tup2<
        T1,
        T2,
    >: ::rkyv::Serialize<crate::storage::file::Serializer>
    + crate::storage::file::Deserializable,
{
    fn checkpoint(&self) -> Result<Vec<u8>, crate::Error> {
        let mut s = crate::storage::file::Serializer::default();
        let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
        let data = s.into_serializer().into_inner().into_vec();
        Ok(data)
    }
    fn restore(&mut self, data: &[u8]) -> Result<(), crate::Error> {
        *self = crate::trace::unaligned_deserialize(data);
        Ok(())
    }
}



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
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        f.debug_tuple("Tup0()").finish()
    }
}

impl Copy for Tup0 {}

impl<T1, T2> Tup2<T1, T2> {
    #[inline]
    pub fn as_mut(&mut self) -> (&mut T1, &mut T2) {
        (&mut self.0, &mut self.1)
    }

    #[inline]
    pub fn fst(&self) -> &T1 {
        self.get_0()
    }

    #[inline]
    pub fn fst_mut(&mut self) -> &mut T1 {
        self.get_0_mut()
    }

    #[inline]
    pub fn snd(&self) -> &T2 {
        self.get_1()
    }

    #[inline]
    pub fn snd_mut(&mut self) -> &mut T2 {
        self.get_1_mut()
    }
}

impl<T1: DBData, T2: DBData> ArchivedTup2<T1, T2> {
    #[inline]
    pub fn as_mut(&mut self) -> (&mut T1::Repr, &mut T2::Repr) {
        (&mut self.0, &mut self.1)
    }

    #[inline]
    pub fn fst(&self) -> &T1::Repr {
        &self.0
    }

    #[inline]
    pub fn fst_mut(&mut self) -> &mut T1::Repr {
        &mut self.0
    }

    #[inline]
    pub fn snd(&self) -> &T2::Repr {
        &self.1
    }

    #[inline]
    pub fn snd_mut(&mut self) -> &mut T2::Repr {
        &mut self.1
    }
}