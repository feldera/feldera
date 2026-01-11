#![allow(unused)]

use dbsp::utils::IsNone;
use feldera_macros::IsNone;
use rkyv::{out_field, Archived};
use std::sync::OnceLock;

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
)]
pub struct Tup1<T0>(pub T0);
impl<T0> Tup1<T0> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(t0: T0) -> Self {
        Self(t0)
    }
}
impl<T0> Tup1<T0> {
    pub const NUM_ELEMENTS: usize = 1;

    #[inline]
    pub fn get_0(&self) -> &T0 {
        &self.0
    }
    #[inline]
    pub fn get_0_mut(&mut self) -> &mut T0 {
        &mut self.0
    }
}

type TupBitmap<const N: usize> = ::dbsp::utils::tuple::TupleBitmap<N>;
type TupFormat = ::dbsp::utils::tuple::TupleFormat;
impl<T0> Tup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn choose_format(&self) -> (TupBitmap<1>, TupFormat) {
        let mut bitmap = TupBitmap::<1>::new();
        if ::dbsp::utils::IsNone::is_none(&self.0) {
            bitmap.set_none(0);
        }

        let none_bits = bitmap.count_none(Self::NUM_ELEMENTS);
        if none_bits * 3 > Self::NUM_ELEMENTS {
            (bitmap, TupFormat::Sparse)
        } else {
            (bitmap, TupFormat::Dense)
        }
    }
}
pub struct ArchivedTup1V3<T0>
where
    T0: ::rkyv::Archive,
{
    pub t0: ::rkyv::Archived<T0>,
}
impl<D, T0> ::rkyv::Deserialize<Tup1<T0>, D> for ArchivedTup1V3<T0>
where
    D: ::rkyv::Fallible + ?Sized,
    T0: ::rkyv::Archive,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup1<T0>, D::Error> {
        Ok(Tup1(self.t0.deserialize(deserializer)?))
    }
}
#[derive(Copy, Clone)]
struct Tup1FieldPtr {
    pos: usize,
}
impl ::rkyv::Archive for Tup1FieldPtr {
    type Archived = ::rkyv::rel_ptr::RawRelPtrI32;
    type Resolver = usize;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos, resolver, out);
    }
}
impl<S> ::rkyv::Serialize<S> for Tup1FieldPtr
where
    S: ::rkyv::ser::Serializer + ?Sized,
{
    #[inline]
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(self.pos)
    }
}
#[repr(C)]
pub struct ArchivedTup1Sparse<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    ptrs: ::rkyv::vec::ArchivedVec<::rkyv::rel_ptr::RawRelPtrI32>,
    _phantom: core::marker::PhantomData<fn() -> (T0)>,
}

#[repr(C)]
pub struct ArchivedTup1Dense<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    t0: core::mem::MaybeUninit<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>,
    _phantom: core::marker::PhantomData<fn() -> (T0)>,
}

#[repr(C)]
pub struct ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    format: TupFormat,
    data: ::rkyv::rel_ptr::RawRelPtrI32,
    _phantom: core::marker::PhantomData<fn() -> (T0)>,
}

impl<T0> ArchivedTup1Sparse<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < Tup1::<T0>::NUM_ELEMENTS);
        self.bitmap.is_none(idx)
    }

    #[inline]
    fn idx_for_field(&self, field_idx: usize) -> usize {
        debug_assert!(field_idx < Tup1::<T0>::NUM_ELEMENTS);
        field_idx - self.bitmap.count_none_before(field_idx)
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(0) {
            None
        } else {
            let ptr_idx = self.idx_for_field(0);
            debug_assert!(ptr_idx < self.ptrs.len());
            Some(unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>()
            })
        }
    }
}

impl<T0> ArchivedTup1Dense<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < Tup1::<T0>::NUM_ELEMENTS);
        self.bitmap.is_none(idx)
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(0) {
            None
        } else {
            Some(unsafe { &*self.t0.as_ptr() })
        }
    }
}

impl<T0> ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn sparse(&self) -> &ArchivedTup1Sparse<T0> {
        unsafe { &*self.data.as_ptr().cast::<ArchivedTup1Sparse<T0>>() }
    }

    #[inline]
    fn dense(&self) -> &ArchivedTup1Dense<T0> {
        unsafe { &*self.data.as_ptr().cast::<ArchivedTup1Dense<T0>>() }
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        match self.format {
            TupFormat::Sparse => self.sparse().get_t0(),
            TupFormat::Dense => self.dense().get_t0(),
        }
    }
}
impl<T0> core::cmp::PartialEq for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        true && self.get_t0() == other.get_t0()
    }
}
impl<T0> core::cmp::Eq for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Eq,
{
}
impl<T0> core::cmp::PartialOrd for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T0> core::cmp::Ord for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let cmp = self.get_t0().cmp(&other.get_t0());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        core::cmp::Ordering::Equal
    }
}
struct Tup1SparseData<'a, T0> {
    bitmap: TupBitmap<1>,
    value: &'a Tup1<T0>,
}

struct Tup1SparseResolver {
    bitmap: TupBitmap<1>,
    ptrs_resolver: ::rkyv::vec::VecResolver,
    ptrs_len: usize,
}

impl<'a, T0> ::rkyv::Archive for Tup1SparseData<'a, T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup1Sparse<T0>;
    type Resolver = Tup1SparseResolver;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
        fo.write(resolver.bitmap);
        let (fp, fo) = ::rkyv::out_field!(out.ptrs);
        let vec_pos = pos + fp;
        ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::resolve_from_len(
            resolver.ptrs_len,
            vec_pos,
            resolver.ptrs_resolver,
            fo,
        );
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<'a, S, T0> ::rkyv::Serialize<S> for Tup1SparseData<'a, T0>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let t0_is_none = self.bitmap.is_none(0);
        let mut ptrs: [Tup1FieldPtr; 1usize] = [Tup1FieldPtr { pos: 0 }; 1usize];
        let mut ptrs_len = 0usize;
        if !t0_is_none {
            let pos =
                serializer.serialize_value(::dbsp::utils::IsNone::unwrap_or_self(&self.value.0))?;
            ptrs[ptrs_len] = Tup1FieldPtr { pos };
            ptrs_len += 1;
        }
        let ptrs_resolver =
            ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::serialize_from_slice(
                &ptrs[..ptrs_len],
                serializer,
            )?;
        Ok(Tup1SparseResolver {
            bitmap: self.bitmap,
            ptrs_resolver,
            ptrs_len,
        })
    }
}

struct Tup1DenseData<'a, T0> {
    bitmap: TupBitmap<1>,
    value: &'a Tup1<T0>,
}

struct Tup1DenseResolver<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    t0: Option<<<T0 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::Resolver>,
}

impl<'a, T0> ::rkyv::Archive for Tup1DenseData<'a, T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup1Dense<T0>;
    type Resolver = Tup1DenseResolver<T0>;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
        fo.write(resolver.bitmap);
        let (fp, fo) = ::rkyv::out_field!(out.t0);
        let t0_out = fo
            .cast::<core::mem::MaybeUninit<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>>(
            );
        if let Some(resolver) = resolver.t0 {
            let inner = ::dbsp::utils::IsNone::unwrap_or_self(&self.value.0);
            <<T0 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::resolve(
                inner,
                pos + fp,
                resolver,
                t0_out.cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>(),
            );
        } else {
            core::ptr::write(t0_out, core::mem::MaybeUninit::zeroed());
        }
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<'a, S, T0> ::rkyv::Serialize<S> for Tup1DenseData<'a, T0>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let t0_is_none = self.bitmap.is_none(0);
        let t0 = if t0_is_none {
            None
        } else {
            Some(::rkyv::Serialize::serialize(
                ::dbsp::utils::IsNone::unwrap_or_self(&self.value.0),
                serializer,
            )?)
        };
        Ok(Tup1DenseResolver {
            bitmap: self.bitmap,
            t0,
        })
    }
}

pub enum Tup1Resolver<T0> {
    Sparse { data_pos: usize },
    Dense { data_pos: usize },
    _Phantom(core::marker::PhantomData<T0>),
}

impl<T0> ::rkyv::Archive for Tup1<T0>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup1<T0>;
    type Resolver = Tup1Resolver<T0>;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, format_out) = ::rkyv::out_field!(out.format);
        let (fp, data_out) = ::rkyv::out_field!(out.data);
        let data_out = data_out.cast::<::rkyv::rel_ptr::RawRelPtrI32>();

        match resolver {
            Tup1Resolver::Sparse { data_pos } => {
                format_out.write(TupFormat::Sparse);
                ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
            }
            Tup1Resolver::Dense { data_pos } => {
                format_out.write(TupFormat::Dense);
                ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
            }
            Tup1Resolver::_Phantom(_) => unreachable!(),
        }
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<S, T0> ::rkyv::Serialize<S> for Tup1<T0>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let (bitmap, format) = self.choose_format();
        match format {
            TupFormat::Dense => {
                let data = Tup1DenseData {
                    bitmap,
                    value: self,
                };
                let data_pos = serializer.serialize_value(&data)?;
                Ok(Tup1Resolver::Dense { data_pos })
            }
            TupFormat::Sparse => {
                let data = Tup1SparseData {
                    bitmap,
                    value: self,
                };
                let data_pos = serializer.serialize_value(&data)?;
                Ok(Tup1Resolver::Sparse { data_pos })
            }
        }
    }
}
impl<D, T0> ::rkyv::Deserialize<Tup1<T0>, D> for ArchivedTup1<T0>
where
    D: ::rkyv::Fallible + ::core::any::Any,
    T0: ::rkyv::Archive + Default + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>:
        ::rkyv::Deserialize<<T0 as ::dbsp::utils::IsNone>::Inner, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup1<T0>, D::Error> {
        let version = (deserializer as &mut dyn ::core::any::Any)
            .downcast_mut::<::dbsp::storage::file::Deserializer>()
            .map(|deserializer| deserializer.version())
            .expect("passed wrong deserializer");
        if version <= 3 {
            let legacy = unsafe { &*(self as *const _ as *const ArchivedTup1V3<T0>) };
            return <ArchivedTup1V3<T0> as ::rkyv::Deserialize<Tup1<T0>, D>>::deserialize(
                legacy,
                deserializer,
            );
        }
        match self.format {
            TupFormat::Sparse => {
                let sparse = self.sparse();
                let mut ptr_idx = 0usize;
                let t0 = if sparse.none_bit_set(0) {
                    T0::default()
                } else {
                    let archived: &::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner> = unsafe {
                        &*sparse
                            .ptrs
                            .as_slice()
                            .get_unchecked(ptr_idx)
                            .as_ptr()
                            .cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>()
                    };
                    ptr_idx += 1;
                    let inner = archived.deserialize(deserializer)?;
                    <T0 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                Ok(Tup1(t0))
            }
            TupFormat::Dense => {
                let dense = self.dense();
                let t0 = if dense.none_bit_set(0) {
                    T0::default()
                } else {
                    let archived = dense.get_t0().expect("ArchivedTup1Dense: missing field 0");
                    let inner = archived.deserialize(deserializer)?;
                    <T0 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                Ok(Tup1(t0))
            }
        }
    }
}
impl<T0, W> ::dbsp::algebra::MulByRef<W> for Tup1<T0>
where
    T0: ::dbsp::algebra::MulByRef<W, Output = T0>,
    W: ::dbsp::algebra::ZRingValue,
{
    type Output = Self;
    fn mul_by_ref(&self, other: &W) -> Self::Output {
        let Tup1(t0) = self;
        Tup1(t0.mul_by_ref(other))
    }
}
impl<T0> ::dbsp::algebra::HasZero for Tup1<T0>
where
    T0: ::dbsp::algebra::HasZero,
{
    fn zero() -> Self {
        Tup1(T0::zero())
    }
    fn is_zero(&self) -> bool {
        let mut result = true;
        let Tup1(t0) = self;
        result = result && t0.is_zero();
        result
    }
}
impl<T0> ::dbsp::algebra::AddByRef for Tup1<T0>
where
    T0: ::dbsp::algebra::AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        let Tup1(t0) = self;
        let Tup1(other_t0) = other;
        Tup1(t0.add_by_ref(other_t0))
    }
}
impl<T0> ::dbsp::algebra::AddAssignByRef for Tup1<T0>
where
    T0: ::dbsp::algebra::AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        let Tup1(ref mut t0) = self;
        let Tup1(ref other_t0) = other;
        t0.add_assign_by_ref(other_t0);
    }
}
impl<T0> ::dbsp::algebra::NegByRef for Tup1<T0>
where
    T0: ::dbsp::algebra::NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        let Tup1(t0) = self;
        Tup1(t0.neg_by_ref())
    }
}
impl<T0> From<(T0)> for Tup1<T0> {
    fn from((t0): (T0)) -> Self {
        Self(t0)
    }
}
impl<'a, T0> Into<(&'a T0,)> for &'a Tup1<T0> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (&'a T0,) {
        let Tup1(t0) = &self;
        (t0,)
    }
}
impl<T0> Into<(T0,)> for Tup1<T0> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (T0,) {
        let Tup1(t0) = self;
        (t0,)
    }
}
impl<T0> ::dbsp::NumEntries for Tup1<T0>
where
    T0: ::dbsp::NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;
    fn num_entries_shallow(&self) -> usize {
        Self::NUM_ELEMENTS
    }
    fn num_entries_deep(&self) -> usize {
        let Tup1(t0) = self;
        0 + (t0).num_entries_deep()
    }
}
impl<T0: core::fmt::Debug> core::fmt::Debug for Tup1<T0> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        let Tup1(t0) = self;
        f.debug_tuple("").field(&t0).finish()
    }
}
impl<T0: Copy> Copy for Tup1<T0> {}
impl<T0> ::dbsp::circuit::checkpointer::Checkpoint for Tup1<T0>
where
    Tup1<T0>: ::rkyv::Serialize<::dbsp::storage::file::Serializer>
        + ::dbsp::storage::file::Deserializable,
{
    fn checkpoint(&self) -> Result<Vec<u8>, ::dbsp::Error> {
        let mut s = ::dbsp::storage::file::Serializer::default();
        let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
        let data = s.into_serializer().into_inner().into_vec();
        Ok(data)
    }
    fn restore(&mut self, data: &[u8]) -> Result<(), ::dbsp::Error> {
        *self = ::dbsp::trace::unaligned_deserialize(data);
        Ok(())
    }
}
impl<T0> ::dbsp::utils::IsNone for Tup1<T0> {
    type Inner = Self;
    fn is_none(&self) -> bool {
        false
    }
    fn unwrap_or_self(&self) -> &Self::Inner {
        self
    }
    fn from_inner(inner: Self::Inner) -> Self {
        inner
    }
}

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
)]
pub struct Tup2<T0, T1>(pub T0, pub T1);
impl<T0, T1> Tup2<T0, T1> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(t0: T0, t1: T1) -> Self {
        Self(t0, t1)
    }
}
impl<T0, T1> Tup2<T0, T1> {
    pub const NUM_ELEMENTS: usize = 2;

    #[inline]
    pub fn get_0(&self) -> &T0 {
        &self.0
    }
    #[inline]
    pub fn get_0_mut(&mut self) -> &mut T0 {
        &mut self.0
    }
}
impl<T0, T1> Tup2<T0, T1> {
    #[inline]
    pub fn get_1(&self) -> &T1 {
        &self.1
    }
    #[inline]
    pub fn get_1_mut(&mut self) -> &mut T1 {
        &mut self.1
    }
}
impl<T0, T1> Tup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn choose_format(&self) -> (TupBitmap<1>, TupFormat) {
        let mut bitmap = TupBitmap::<1>::new();
        if ::dbsp::utils::IsNone::is_none(&self.0) {
            bitmap.set_none(0);
        }
        if ::dbsp::utils::IsNone::is_none(&self.1) {
            bitmap.set_none(1);
        }

        let none_bits = bitmap.count_none(Self::NUM_ELEMENTS);
        if none_bits * 3 > Self::NUM_ELEMENTS {
            eprintln!("choosing TupFormat::Sparse...");
            (bitmap, TupFormat::Sparse)
        } else {
            eprintln!("choosing TupFormat::Dense");
            (bitmap, TupFormat::Dense)
        }
    }
}
pub struct ArchivedTup2V3<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
{
    pub t0: ::rkyv::Archived<T0>,
    pub t1: ::rkyv::Archived<T1>,
}
impl<D, T0, T1> ::rkyv::Deserialize<Tup2<T0, T1>, D> for ArchivedTup2V3<T0, T1>
where
    D: ::rkyv::Fallible + ?Sized,
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
    ::rkyv::Archived<T1>: ::rkyv::Deserialize<T1, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup2<T0, T1>, D::Error> {
        Ok(Tup2(
            self.t0.deserialize(deserializer)?,
            self.t1.deserialize(deserializer)?,
        ))
    }
}
#[derive(Copy, Clone)]
struct Tup2FieldPtr {
    pos: usize,
}
impl ::rkyv::Archive for Tup2FieldPtr {
    type Archived = ::rkyv::rel_ptr::RawRelPtrI32;
    type Resolver = usize;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos, resolver, out);
    }
}
impl<S> ::rkyv::Serialize<S> for Tup2FieldPtr
where
    S: ::rkyv::ser::Serializer + ?Sized,
{
    #[inline]
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(self.pos)
    }
}
#[repr(C)]
pub struct ArchivedTup2Sparse<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    ptrs: ::rkyv::vec::ArchivedVec<::rkyv::rel_ptr::RawRelPtrI32>,
    _phantom: core::marker::PhantomData<fn() -> (T0, T1)>,
}

#[repr(C)]
pub struct ArchivedTup2Dense<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    t0: core::mem::MaybeUninit<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>,
    t1: core::mem::MaybeUninit<::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>>,
    _phantom: core::marker::PhantomData<fn() -> (T0, T1)>,
}

#[repr(C)]
pub struct ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    format: TupFormat,
    data: ::rkyv::rel_ptr::RawRelPtrI32,
    _phantom: core::marker::PhantomData<fn() -> (T0, T1)>,
}

impl<T0, T1> ArchivedTup2Sparse<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < Tup2::<T0, T1>::NUM_ELEMENTS);
        self.bitmap.is_none(idx)
    }

    #[inline]
    fn idx_for_field(&self, field_idx: usize) -> usize {
        debug_assert!(field_idx < Tup2::<T0, T1>::NUM_ELEMENTS);
        field_idx - self.bitmap.count_none_before(field_idx)
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(0) {
            None
        } else {
            let ptr_idx = self.idx_for_field(0);
            debug_assert!(ptr_idx < self.ptrs.len());
            Some(unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>()
            })
        }
    }

    #[inline]
    pub fn get_t1(&self) -> Option<&::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(1) {
            None
        } else {
            let ptr_idx = self.idx_for_field(1);
            debug_assert!(ptr_idx < self.ptrs.len());
            Some(unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>>()
            })
        }
    }
}

impl<T0, T1> ArchivedTup2Dense<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < Tup2::<T0, T1>::NUM_ELEMENTS);
        self.bitmap.is_none(idx)
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(0) {
            None
        } else {
            Some(unsafe { &*self.t0.as_ptr() })
        }
    }

    #[inline]
    pub fn get_t1(&self) -> Option<&::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>> {
        if self.none_bit_set(1) {
            None
        } else {
            Some(unsafe { &*self.t1.as_ptr() })
        }
    }
}

impl<T0, T1> ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    #[inline]
    fn sparse(&self) -> &ArchivedTup2Sparse<T0, T1> {
        unsafe { &*self.data.as_ptr().cast::<ArchivedTup2Sparse<T0, T1>>() }
    }

    #[inline]
    fn dense(&self) -> &ArchivedTup2Dense<T0, T1> {
        unsafe { &*self.data.as_ptr().cast::<ArchivedTup2Dense<T0, T1>>() }
    }

    #[inline]
    pub fn get_t0(&self) -> Option<&::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>> {
        match self.format {
            TupFormat::Sparse => self.sparse().get_t0(),
            TupFormat::Dense => self.dense().get_t0(),
        }
    }

    #[inline]
    pub fn get_t1(&self) -> Option<&::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>> {
        match self.format {
            TupFormat::Sparse => self.sparse().get_t1(),
            TupFormat::Dense => self.dense().get_t1(),
        }
    }
}
impl<T0, T1> core::cmp::PartialEq for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,
    ::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        true && self.get_t0() == other.get_t0() && self.get_t1() == other.get_t1()
    }
}
impl<T0, T1> core::cmp::Eq for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Eq,
    ::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Eq,
{
}
impl<T0, T1> core::cmp::PartialOrd for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
    ::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T0, T1> core::cmp::Ord for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
    ::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let cmp = self.get_t0().cmp(&other.get_t0());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        let cmp = self.get_t1().cmp(&other.get_t1());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        core::cmp::Ordering::Equal
    }
}
struct Tup2SparseData<'a, T0, T1> {
    bitmap: TupBitmap<1>,
    value: &'a Tup2<T0, T1>,
}

struct Tup2SparseResolver {
    bitmap: TupBitmap<1>,
    ptrs_resolver: ::rkyv::vec::VecResolver,
    ptrs_len: usize,
}

impl<'a, T0, T1> ::rkyv::Archive for Tup2SparseData<'a, T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup2Sparse<T0, T1>;
    type Resolver = Tup2SparseResolver;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
        fo.write(resolver.bitmap);
        let (fp, fo) = ::rkyv::out_field!(out.ptrs);
        let vec_pos = pos + fp;
        ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::resolve_from_len(
            resolver.ptrs_len,
            vec_pos,
            resolver.ptrs_resolver,
            fo,
        );
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<'a, S, T0, T1> ::rkyv::Serialize<S> for Tup2SparseData<'a, T0, T1>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let t0_is_none = self.bitmap.is_none(0);
        let t1_is_none = self.bitmap.is_none(1);
        let mut ptrs: [Tup2FieldPtr; 2usize] = [Tup2FieldPtr { pos: 0 }; 2usize];
        let mut ptrs_len = 0usize;
        if !t0_is_none {
            let pos =
                serializer.serialize_value(::dbsp::utils::IsNone::unwrap_or_self(&self.value.0))?;
            ptrs[ptrs_len] = Tup2FieldPtr { pos };
            ptrs_len += 1;
        }
        if !t1_is_none {
            let pos =
                serializer.serialize_value(::dbsp::utils::IsNone::unwrap_or_self(&self.value.1))?;
            ptrs[ptrs_len] = Tup2FieldPtr { pos };
            ptrs_len += 1;
        }
        let ptrs_resolver =
            ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::serialize_from_slice(
                &ptrs[..ptrs_len],
                serializer,
            )?;
        Ok(Tup2SparseResolver {
            bitmap: self.bitmap,
            ptrs_resolver,
            ptrs_len,
        })
    }
}

struct Tup2DenseData<'a, T0, T1> {
    bitmap: TupBitmap<1>,
    value: &'a Tup2<T0, T1>,
}

struct Tup2DenseResolver<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    bitmap: TupBitmap<1>,
    t0: Option<<<T0 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::Resolver>,
    t1: Option<<<T1 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::Resolver>,
}

impl<'a, T0, T1> ::rkyv::Archive for Tup2DenseData<'a, T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup2Dense<T0, T1>;
    type Resolver = Tup2DenseResolver<T0, T1>;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, fo) = ::rkyv::out_field!(out.bitmap);
        fo.write(resolver.bitmap);
        let (fp, fo) = ::rkyv::out_field!(out.t0);
        let t0_out = fo
            .cast::<core::mem::MaybeUninit<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>>(
            );
        if let Some(resolver) = resolver.t0 {
            let inner = ::dbsp::utils::IsNone::unwrap_or_self(&self.value.0);
            <<T0 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::resolve(
                inner,
                pos + fp,
                resolver,
                t0_out.cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>(),
            );
        } else {
            core::ptr::write(t0_out, core::mem::MaybeUninit::zeroed());
        }
        let (fp, fo) = ::rkyv::out_field!(out.t1);
        let t1_out = fo
            .cast::<core::mem::MaybeUninit<::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>>>(
            );
        if let Some(resolver) = resolver.t1 {
            let inner = ::dbsp::utils::IsNone::unwrap_or_self(&self.value.1);
            <<T1 as ::dbsp::utils::IsNone>::Inner as ::rkyv::Archive>::resolve(
                inner,
                pos + fp,
                resolver,
                t1_out.cast::<::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>>(),
            );
        } else {
            core::ptr::write(t1_out, core::mem::MaybeUninit::zeroed());
        }
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<'a, S, T0, T1> ::rkyv::Serialize<S> for Tup2DenseData<'a, T0, T1>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let t0_is_none = self.bitmap.is_none(0);
        let t1_is_none = self.bitmap.is_none(1);
        let t0 = if t0_is_none {
            None
        } else {
            Some(::rkyv::Serialize::serialize(
                ::dbsp::utils::IsNone::unwrap_or_self(&self.value.0),
                serializer,
            )?)
        };
        let t1 = if t1_is_none {
            None
        } else {
            Some(::rkyv::Serialize::serialize(
                ::dbsp::utils::IsNone::unwrap_or_self(&self.value.1),
                serializer,
            )?)
        };
        Ok(Tup2DenseResolver {
            bitmap: self.bitmap,
            t0,
            t1,
        })
    }
}

pub enum Tup2Resolver<T0, T1> {
    Sparse { data_pos: usize },
    Dense { data_pos: usize },
    _Phantom(core::marker::PhantomData<(T0, T1)>),
}

impl<T0, T1> ::rkyv::Archive for Tup2<T0, T1>
where
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
{
    type Archived = ArchivedTup2<T0, T1>;
    type Resolver = Tup2Resolver<T0, T1>;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (_fp, format_out) = ::rkyv::out_field!(out.format);
        let (fp, data_out) = ::rkyv::out_field!(out.data);
        let data_out = data_out.cast::<::rkyv::rel_ptr::RawRelPtrI32>();

        match resolver {
            Tup2Resolver::Sparse { data_pos } => {
                format_out.write(TupFormat::Sparse);
                ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
            }
            Tup2Resolver::Dense { data_pos } => {
                format_out.write(TupFormat::Dense);
                ::rkyv::rel_ptr::RawRelPtrI32::emplace(pos + fp, data_pos, data_out);
            }
            Tup2Resolver::_Phantom(_) => unreachable!(),
        }
        let (_fp, fo) = ::rkyv::out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<S, T0, T1> ::rkyv::Serialize<S> for Tup2<T0, T1>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive + ::rkyv::Serialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let (bitmap, format) = self.choose_format();
        match format {
            TupFormat::Dense => {
                let data = Tup2DenseData {
                    bitmap,
                    value: self,
                };
                let data_pos = serializer.serialize_value(&data)?;
                Ok(Tup2Resolver::Dense { data_pos })
            }
            TupFormat::Sparse => {
                let data = Tup2SparseData {
                    bitmap,
                    value: self,
                };
                let data_pos = serializer.serialize_value(&data)?;
                Ok(Tup2Resolver::Sparse { data_pos })
            }
        }
    }
}
impl<D, T0, T1> ::rkyv::Deserialize<Tup2<T0, T1>, D> for ArchivedTup2<T0, T1>
where
    D: ::rkyv::Fallible + ::core::any::Any,
    T0: ::rkyv::Archive + Default + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + Default + ::dbsp::utils::IsNone,
    <T0 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    <T1 as ::dbsp::utils::IsNone>::Inner: ::rkyv::Archive,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
    ::rkyv::Archived<T1>: ::rkyv::Deserialize<T1, D>,
    ::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>:
        ::rkyv::Deserialize<<T0 as ::dbsp::utils::IsNone>::Inner, D>,
    ::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>:
        ::rkyv::Deserialize<<T1 as ::dbsp::utils::IsNone>::Inner, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup2<T0, T1>, D::Error> {
        let version = (deserializer as &mut dyn ::core::any::Any)
            .downcast_mut::<::dbsp::storage::file::Deserializer>()
            .map(|deserializer| deserializer.version())
            .expect("passed wrong deserializer");
        if version <= 3 {
            let legacy = unsafe { &*(self as *const _ as *const ArchivedTup2V3<T0, T1>) };
            return <ArchivedTup2V3<T0, T1> as ::rkyv::Deserialize<Tup2<T0, T1>, D>>::deserialize(
                legacy,
                deserializer,
            );
        }
        match self.format {
            TupFormat::Sparse => {
                let sparse = self.sparse();
                let mut ptr_idx = 0usize;
                let t0 = if sparse.none_bit_set(0) {
                    T0::default()
                } else {
                    let archived: &::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner> = unsafe {
                        &*sparse
                            .ptrs
                            .as_slice()
                            .get_unchecked(ptr_idx)
                            .as_ptr()
                            .cast::<::rkyv::Archived<<T0 as ::dbsp::utils::IsNone>::Inner>>()
                    };
                    ptr_idx += 1;
                    let inner = archived.deserialize(deserializer)?;
                    <T0 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                let t1 = if sparse.none_bit_set(1) {
                    T1::default()
                } else {
                    let archived: &::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner> = unsafe {
                        &*sparse
                            .ptrs
                            .as_slice()
                            .get_unchecked(ptr_idx)
                            .as_ptr()
                            .cast::<::rkyv::Archived<<T1 as ::dbsp::utils::IsNone>::Inner>>()
                    };
                    ptr_idx += 1;
                    let inner = archived.deserialize(deserializer)?;
                    <T1 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                Ok(Tup2(t0, t1))
            }
            TupFormat::Dense => {
                let dense = self.dense();
                let t0 = if dense.none_bit_set(0) {
                    T0::default()
                } else {
                    let archived = dense.get_t0().expect("ArchivedTup2Dense: missing field 0");
                    let inner = archived.deserialize(deserializer)?;
                    <T0 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                let t1 = if dense.none_bit_set(1) {
                    T1::default()
                } else {
                    let archived = dense.get_t1().expect("ArchivedTup2Dense: missing field 1");
                    let inner = archived.deserialize(deserializer)?;
                    <T1 as ::dbsp::utils::IsNone>::from_inner(inner)
                };
                Ok(Tup2(t0, t1))
            }
        }
    }
}
impl<T0, T1, W> ::dbsp::algebra::MulByRef<W> for Tup2<T0, T1>
where
    T0: ::dbsp::algebra::MulByRef<W, Output = T0>,
    T1: ::dbsp::algebra::MulByRef<W, Output = T1>,
    W: ::dbsp::algebra::ZRingValue,
{
    type Output = Self;
    fn mul_by_ref(&self, other: &W) -> Self::Output {
        let Tup2(t0, t1) = self;
        Tup2(t0.mul_by_ref(other), t1.mul_by_ref(other))
    }
}
impl<T0, T1> ::dbsp::algebra::HasZero for Tup2<T0, T1>
where
    T0: ::dbsp::algebra::HasZero,
    T1: ::dbsp::algebra::HasZero,
{
    fn zero() -> Self {
        Tup2(T0::zero(), T1::zero())
    }
    fn is_zero(&self) -> bool {
        let mut result = true;
        let Tup2(t0, t1) = self;
        result = result && t0.is_zero();
        result = result && t1.is_zero();
        result
    }
}
impl<T0, T1> ::dbsp::algebra::AddByRef for Tup2<T0, T1>
where
    T0: ::dbsp::algebra::AddByRef,
    T1: ::dbsp::algebra::AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        let Tup2(t0, t1) = self;
        let Tup2(other_t0, other_t1) = other;
        Tup2(t0.add_by_ref(other_t0), t1.add_by_ref(other_t1))
    }
}
impl<T0, T1> ::dbsp::algebra::AddAssignByRef for Tup2<T0, T1>
where
    T0: ::dbsp::algebra::AddAssignByRef,
    T1: ::dbsp::algebra::AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        let Tup2(ref mut t0, ref mut t1) = self;
        let Tup2(ref other_t0, ref other_t1) = other;
        t0.add_assign_by_ref(other_t0);
        t1.add_assign_by_ref(other_t1);
    }
}
impl<T0, T1> ::dbsp::algebra::NegByRef for Tup2<T0, T1>
where
    T0: ::dbsp::algebra::NegByRef,
    T1: ::dbsp::algebra::NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        let Tup2(t0, t1) = self;
        Tup2(t0.neg_by_ref(), t1.neg_by_ref())
    }
}
impl<T0, T1> From<(T0, T1)> for Tup2<T0, T1> {
    fn from((t0, t1): (T0, T1)) -> Self {
        Self(t0, t1)
    }
}
impl<'a, T0, T1> Into<(&'a T0, &'a T1)> for &'a Tup2<T0, T1> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (&'a T0, &'a T1) {
        let Tup2(t0, t1) = &self;
        (t0, t1)
    }
}
impl<T0, T1> Into<(T0, T1)> for Tup2<T0, T1> {
    #[allow(clippy::from_over_into)]
    fn into(self) -> (T0, T1) {
        let Tup2(t0, t1) = self;
        (t0, t1)
    }
}
impl<T0, T1> ::dbsp::NumEntries for Tup2<T0, T1>
where
    T0: ::dbsp::NumEntries,
    T1: ::dbsp::NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;
    fn num_entries_shallow(&self) -> usize {
        Self::NUM_ELEMENTS
    }
    fn num_entries_deep(&self) -> usize {
        let Tup2(t0, t1) = self;
        0 + (t0).num_entries_deep() + (t1).num_entries_deep()
    }
}
impl<T0: core::fmt::Debug, T1: core::fmt::Debug> core::fmt::Debug for Tup2<T0, T1> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        let Tup2(t0, t1) = self;
        f.debug_tuple("").field(&t0).field(&t1).finish()
    }
}
impl<T0: Copy, T1: Copy> Copy for Tup2<T0, T1> {}
impl<T0, T1> ::dbsp::circuit::checkpointer::Checkpoint for Tup2<T0, T1>
where
    Tup2<T0, T1>: ::rkyv::Serialize<::dbsp::storage::file::Serializer>
        + ::dbsp::storage::file::Deserializable,
{
    fn checkpoint(&self) -> Result<Vec<u8>, ::dbsp::Error> {
        let mut s = ::dbsp::storage::file::Serializer::default();
        let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
        let data = s.into_serializer().into_inner().into_vec();
        Ok(data)
    }
    fn restore(&mut self, data: &[u8]) -> Result<(), ::dbsp::Error> {
        *self = ::dbsp::trace::unaligned_deserialize(data);
        Ok(())
    }
}
impl<T0, T1> ::dbsp::utils::IsNone for Tup2<T0, T1> {
    type Inner = Self;
    fn is_none(&self) -> bool {
        false
    }
    fn unwrap_or_self(&self) -> &Self::Inner {
        self
    }
    fn from_inner(inner: Self::Inner) -> Self {
        inner
    }
}

#[test]
fn compile_tup1() {
    let t1 = Tup1::new(Some(32i32));
}

#[test]
fn compile_tup2() {
    let t2 = Tup2::new(Some(32i32), Some(64i32));
}

#[test]
fn rkyv_compact_tup1_option_roundtrip_bool() {
    let tup_some = Tup1::new(Some(true));
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup1<Option<bool>> = dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    let tup_none: Tup1<Option<bool>> = Tup1::new(None);
    let bytes_none = dbsp::storage::file::to_bytes(&tup_none).unwrap();
    let restored_none: Tup1<Option<bool>> = dbsp::trace::unaligned_deserialize(&bytes_none[..]);
    assert_eq!(restored_none, tup_none);
    assert!(
        bytes_none.len() >= core::mem::size_of::<ArchivedTup1<Option<bool>>>(),
        "expected serialized bytes to include archived data, got bytes={} archived={}",
        bytes_none.len(),
        core::mem::size_of::<ArchivedTup1<Option<bool>>>()
    );

    eprintln!(
        "Tup1<Option<bool>>: None={} Some={}",
        bytes_none.len(),
        bytes_some.len()
    );

    let _ = (bytes_none, bytes_some);
}

#[test]
fn rkyv_compact_tup1_option_roundtrip() {
    use feldera_sqllib::SqlString;

    let tup_some = Tup1::new(());
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup1<()> = dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    eprintln!("Tup1<()>: {}", bytes_some.len());

    let tup_some = Tup1::new(0u8);
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup1<u8> = dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    eprintln!("Tup1<u8>: {}", bytes_some.len());

    let tup_some = Tup1::new(Some(0u8));
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup1<Option<u8>> = dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    eprintln!("Tup1<Option<u8>>: {}", bytes_some.len());
}

#[test]
fn rkyv_compact_tup2_option_sizes() {
    use feldera_sqllib::SqlString;

    let tup_some = Tup2::new(
        Some(SqlString::from_ref("hello")),
        Some(SqlString::from_ref("hello")),
    );
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup2<Option<SqlString>, Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    let tup_none: Tup2<Option<SqlString>, Option<SqlString>> = Tup2::new(None, None);
    let bytes_none = dbsp::storage::file::to_bytes(&tup_none).unwrap();
    let restored_none: Tup2<Option<SqlString>, Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes_none[..]);
    assert_eq!(restored_none, tup_none);

    eprintln!(
        "Tup2<Option<SqlString>, Option<SqlString>>: None={} Some={}",
        bytes_none.len(),
        bytes_some.len()
    );
}

#[test]
fn rkyv_compact_tup1_archived_get_t0() {
    use feldera_sqllib::SqlString;

    let tup_none: Tup1<Option<SqlString>> = Tup1::new(None);
    let bytes_none = dbsp::storage::file::to_bytes(&tup_none).unwrap();
    let archived_none = unsafe { rkyv::archived_root::<Tup1<Option<SqlString>>>(&bytes_none[..]) };
    let archived_t0_none = archived_none.get_t0();
    assert!(archived_t0_none.is_none());

    let tup_some = Tup1::new(Some(SqlString::from_ref("hello")));
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let archived_some = unsafe { rkyv::archived_root::<Tup1<Option<SqlString>>>(&bytes_some[..]) };
    let archived_t0_some = archived_some.get_t0();
    assert!(archived_t0_some.is_some());
    assert_eq!(archived_t0_some.unwrap().as_str(), "hello");
}

#[test]
fn rkyv_compact_tup1_option_checkpoint_roundtrip() {
    use dbsp::circuit::checkpointer::Checkpoint;
    use feldera_sqllib::SqlString;

    let tup_some = Tup1::new(Some(SqlString::from_ref("hello")));
    let bytes_some = tup_some.checkpoint().unwrap();
    let mut restored_some: Tup1<Option<SqlString>> = Tup1::new(None);
    restored_some.restore(&bytes_some).unwrap();
    assert_eq!(restored_some, tup_some);

    let tup_none: Tup1<Option<SqlString>> = Tup1::new(None);
    let bytes_none = tup_none.checkpoint().unwrap();
    let mut restored_none: Tup1<Option<SqlString>> = Tup1::new(Some(SqlString::from_ref("x")));
    restored_none.restore(&bytes_none).unwrap();
    assert_eq!(restored_none, tup_none);

    assert!(
        bytes_none.len() >= core::mem::size_of::<ArchivedTup1<Option<SqlString>>>(),
        "expected serialized bytes to include archived data, got bytes={} archived={}",
        bytes_none.len(),
        core::mem::size_of::<ArchivedTup1<Option<SqlString>>>()
    );
}

#[test]
fn rkyv_compact_tup2_roundtrip() {
    use feldera_sqllib::SqlString;

    let tup_vals: Tup2<SqlString, i32> = Tup2::new(SqlString::from_ref("hi"), 8i32);
    let bytes_vals = dbsp::storage::file::to_bytes(&tup_vals).unwrap();
    let restored_vals: Tup2<SqlString, i32> = dbsp::trace::unaligned_deserialize(&bytes_vals[..]);
    assert_eq!(restored_vals, tup_vals);
    assert!(
        bytes_vals.len() >= core::mem::size_of::<ArchivedTup2<SqlString, i32>>(),
        "expected serialized bytes to include archived data, got bytes={} archived={}",
        bytes_vals.len(),
        core::mem::size_of::<ArchivedTup2<SqlString, i32>>()
    );
}

#[test]
fn rkyv_compact_tup2_option_roundtrip() {
    use feldera_sqllib::SqlString;

    let tup_none: Tup2<Option<SqlString>, Option<u128>> = Tup2::new(None, None);
    let bytes_none = dbsp::storage::file::to_bytes(&tup_none).unwrap();
    let restored_none: Tup2<Option<SqlString>, Option<u128>> =
        dbsp::trace::unaligned_deserialize(&bytes_none[..]);
    assert_eq!(restored_none, tup_none);
    eprintln!("bytes_none.len() = {}", bytes_none.len());
    assert!(
        bytes_none.len() >= core::mem::size_of::<ArchivedTup2<Option<SqlString>, Option<u128>>>(),
        "expected serialized bytes to include archived data, got bytes={} archived={}",
        bytes_none.len(),
        core::mem::size_of::<ArchivedTup2<Option<SqlString>, Option<u128>>>()
    );

    let tup_10: Tup2<Option<SqlString>, Option<i32>> =
        Tup2::new(Some(SqlString::from_ref("hi")), None);
    let bytes_10 = dbsp::storage::file::to_bytes(&tup_10).unwrap();
    let restored_10: Tup2<Option<SqlString>, Option<i32>> =
        dbsp::trace::unaligned_deserialize(&bytes_10[..]);
    assert_eq!(restored_10, tup_10);

    let tup_01: Tup2<Option<SqlString>, Option<i32>> = Tup2::new(None, Some(42));
    let bytes_01 = dbsp::storage::file::to_bytes(&tup_01).unwrap();
    let restored_01: Tup2<Option<SqlString>, Option<i32>> =
        dbsp::trace::unaligned_deserialize(&bytes_01[..]);
    assert_eq!(restored_01, tup_01);

    let tup_11: Tup2<Option<SqlString>, Option<i32>> =
        Tup2::new(Some(SqlString::from_ref("hello")), Some(42));
    let bytes_11 = dbsp::storage::file::to_bytes(&tup_11).unwrap();
    let restored_11: Tup2<Option<SqlString>, Option<i32>> =
        dbsp::trace::unaligned_deserialize(&bytes_11[..]);
    assert_eq!(restored_11, tup_11);
}

#[test]
fn rkyv_compact_tup2_archived_get_t0_t1() {
    use feldera_sqllib::SqlString;

    let tup_10: Tup2<Option<SqlString>, Option<i32>> =
        Tup2::new(Some(SqlString::from_ref("hi")), None);
    let bytes_10 = dbsp::storage::file::to_bytes(&tup_10).unwrap();
    let archived_10 =
        unsafe { rkyv::archived_root::<Tup2<Option<SqlString>, Option<i32>>>(&bytes_10[..]) };
    let t0_10 = archived_10.get_t0();
    let t1_10 = archived_10.get_t1();
    assert!(t0_10.is_some());
    assert!(t1_10.is_none());
    assert_eq!(t0_10.unwrap().as_str(), "hi");

    let tup_01: Tup2<Option<SqlString>, Option<i32>> = Tup2::new(None, Some(42));
    let bytes_01 = dbsp::storage::file::to_bytes(&tup_01).unwrap();
    let archived_01 =
        unsafe { rkyv::archived_root::<Tup2<Option<SqlString>, Option<i32>>>(&bytes_01[..]) };
    let t0_01 = archived_01.get_t0();
    let t1_01 = archived_01.get_t1();
    assert!(t0_01.is_none());
    assert!(t1_01.is_some());
    assert_eq!(*t1_01.unwrap(), 42);
}
