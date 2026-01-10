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
    #[inline]
    pub fn get_0(&self) -> &T0 {
        &self.0
    }
    #[inline]
    pub fn get_0_mut(&mut self) -> &mut T0 {
        &mut self.0
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
#[inline]
unsafe fn __tup1_archived_none_ptr<T: ::rkyv::Archive>() -> *const ::rkyv::Archived<T> {
    static NONE: ::std::sync::OnceLock<usize> = ::std::sync::OnceLock::new();
    let ptr = *NONE.get_or_init(|| {
        let boxed: Box<::rkyv::Archived<T>> = Box::new(unsafe { ::core::mem::zeroed() });
        Box::into_raw(boxed) as usize
    });
    ptr as *const ::rkyv::Archived<T>
}
pub struct ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
{
    bitmap: [u64; 1usize],
    ptrs: ::rkyv::vec::ArchivedVec<::rkyv::rel_ptr::RawRelPtrI32>,
    _phantom: core::marker::PhantomData<fn() -> (T0)>,
}
impl<T0> ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < 1usize);
        let word = idx / 64;
        let bit = idx % 64;
        (self.bitmap[word] & (1u64 << bit)) != 0
    }
    #[inline]
    fn idx_for_field(&self, field_idx: usize) -> usize {
        debug_assert!(field_idx < 1usize);
        let word = field_idx / 64;
        let bit = field_idx % 64;
        let mut idx = 0usize;
        let mut w = 0usize;
        while w < word {
            idx += 64usize - (self.bitmap[w].count_ones() as usize);
            w += 1;
        }
        let mask = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
        let none_before = (self.bitmap[word] & mask).count_ones() as usize;
        idx + bit - none_before
    }
    #[inline]
    pub fn get_t0(&self) -> &::rkyv::Archived<T0> {
        if self.none_bit_set(0) {
            unsafe { &*__tup1_archived_none_ptr::<T0>() }
        } else {
            let ptr_idx = self.idx_for_field(0);
            debug_assert!(ptr_idx < self.ptrs.len());
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T0>>()
            }
        }
    }
}
impl<T0> core::cmp::PartialEq for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        true && self.get_t0() == other.get_t0()
    }
}
impl<T0> core::cmp::Eq for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Eq,
{
}
impl<T0> core::cmp::PartialOrd for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T0> core::cmp::Ord for ArchivedTup1<T0>
where
    T0: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let cmp = self.get_t0().cmp(other.get_t0());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        core::cmp::Ordering::Equal
    }
}
pub struct Tup1Resolver {
    bitmap: [u64; 1usize],
    ptrs_resolver: ::rkyv::vec::VecResolver,
    ptrs_len: usize,
}
impl<T0> ::rkyv::Archive for Tup1<T0>
where
    T0: ::rkyv::Archive,
{
    type Archived = ArchivedTup1<T0>;
    type Resolver = Tup1Resolver;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = ::rkyv::out_field!(out.bitmap);
        let bitmap_out = fo.cast::<u64>();
        u64::resolve(
            &resolver.bitmap[0],
            pos + fp + (0 * ::core::mem::size_of::<u64>()),
            (),
            bitmap_out.add(0),
        );
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
impl<S, T0> ::rkyv::Serialize<S> for Tup1<T0>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::rkyv::Serialize<S> + ::dbsp::utils::IsNone,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let mut bitmap = [0u64; 1usize];
        let mut ptrs: [Tup1FieldPtr; 1usize] = [Tup1FieldPtr { pos: 0 }; 1usize];
        let mut ptrs_len = 0usize;
        if ::dbsp::utils::IsNone::is_none(&self.0) {
            bitmap[0] |= 1u64 << 0usize;
        } else {
            let pos = serializer.serialize_value(&self.0)?;
            ptrs[ptrs_len] = Tup1FieldPtr { pos };
            ptrs_len += 1;
        }
        let ptrs_resolver =
            ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::serialize_from_slice(
                &ptrs[..ptrs_len],
                serializer,
            )?;
        Ok(Tup1Resolver {
            bitmap,
            ptrs_resolver,
            ptrs_len,
        })
    }
}
impl<D, T0> ::rkyv::Deserialize<Tup1<T0>, D> for ArchivedTup1<T0>
where
    D: ::rkyv::Fallible + ::core::any::Any,
    T0: ::rkyv::Archive + Default,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup1<T0>, D::Error> {
        let version = (deserializer as &mut dyn ::core::any::Any)
            .downcast_mut::<::dbsp::storage::file::Deserializer>()
            .map(|deserializer| deserializer.version())
            .expect("passed wrong deserializer");
        if version <= 3 {
            let legacy = unsafe { &*(self as *const _ as *const ArchivedTup1V3<T0>) };
            return legacy.deserialize(deserializer);
        }
        let mut ptr_idx = 0usize;
        let t0 = if self.none_bit_set(0) {
            T0::default()
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T0>>()
            };
            ptr_idx += 1;
            archived.deserialize(deserializer)?
        };
        Ok(Tup1(t0))
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
        1usize
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
    fn is_none(&self) -> bool {
        false
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
#[inline]
unsafe fn __tup2_archived_none_ptr<T: ::rkyv::Archive>() -> *const ::rkyv::Archived<T> {
    static NONE: ::std::sync::OnceLock<usize> = ::std::sync::OnceLock::new();
    let ptr = *NONE.get_or_init(|| {
        let boxed: Box<::rkyv::Archived<T>> = Box::new(unsafe { ::core::mem::zeroed() });
        Box::into_raw(boxed) as usize
    });
    ptr as *const ::rkyv::Archived<T>
}
pub struct ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
{
    bitmap: [u64; 1usize],
    ptrs: ::rkyv::vec::ArchivedVec<::rkyv::rel_ptr::RawRelPtrI32>,
    _phantom: core::marker::PhantomData<fn() -> (T0, T1)>,
}
impl<T0, T1> ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < 2usize);
        let word = idx / 64;
        let bit = idx % 64;
        (self.bitmap[word] & (1u64 << bit)) != 0
    }
    #[inline]
    fn idx_for_field(&self, field_idx: usize) -> usize {
        debug_assert!(field_idx < 2usize);
        let word = field_idx / 64;
        let bit = field_idx % 64;
        let mut idx = 0usize;
        let mut w = 0usize;
        while w < word {
            idx += 64usize - (self.bitmap[w].count_ones() as usize);
            w += 1;
        }
        let mask = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
        let none_before = (self.bitmap[word] & mask).count_ones() as usize;
        idx + bit - none_before
    }
    #[inline]
    pub fn get_t0(&self) -> &::rkyv::Archived<T0> {
        if self.none_bit_set(0) {
            unsafe { &*__tup2_archived_none_ptr::<T0>() }
        } else {
            let ptr_idx = self.idx_for_field(0);
            debug_assert!(ptr_idx < self.ptrs.len());
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T0>>()
            }
        }
    }
    #[inline]
    pub fn get_t1(&self) -> &::rkyv::Archived<T1> {
        if self.none_bit_set(1) {
            unsafe { &*__tup2_archived_none_ptr::<T1>() }
        } else {
            let ptr_idx = self.idx_for_field(1);
            debug_assert!(ptr_idx < self.ptrs.len());
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T1>>()
            }
        }
    }
}
impl<T0, T1> core::cmp::PartialEq for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::PartialEq,
    ::rkyv::Archived<T1>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        true && self.get_t0() == other.get_t0() && self.get_t1() == other.get_t1()
    }
}
impl<T0, T1> core::cmp::Eq for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Eq,
    ::rkyv::Archived<T1>: core::cmp::Eq,
{
}
impl<T0, T1> core::cmp::PartialOrd for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Ord,
    ::rkyv::Archived<T1>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T0, T1> core::cmp::Ord for ArchivedTup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
    ::rkyv::Archived<T0>: core::cmp::Ord,
    ::rkyv::Archived<T1>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let cmp = self.get_t0().cmp(other.get_t0());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        let cmp = self.get_t1().cmp(other.get_t1());
        if cmp != core::cmp::Ordering::Equal {
            return cmp;
        }
        core::cmp::Ordering::Equal
    }
}
pub struct Tup2Resolver {
    bitmap: [u64; 1usize],
    ptrs_resolver: ::rkyv::vec::VecResolver,
    ptrs_len: usize,
}
impl<T0, T1> ::rkyv::Archive for Tup2<T0, T1>
where
    T0: ::rkyv::Archive,
    T1: ::rkyv::Archive,
{
    type Archived = ArchivedTup2<T0, T1>;
    type Resolver = Tup2Resolver;
    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = ::rkyv::out_field!(out.bitmap);
        let bitmap_out = fo.cast::<u64>();
        u64::resolve(
            &resolver.bitmap[0],
            pos + fp + (0 * ::core::mem::size_of::<u64>()),
            (),
            bitmap_out.add(0),
        );
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
impl<S, T0, T1> ::rkyv::Serialize<S> for Tup2<T0, T1>
where
    S: ::rkyv::ser::Serializer + ::rkyv::ser::ScratchSpace + ?Sized,
    T0: ::rkyv::Archive + ::rkyv::Serialize<S> + ::dbsp::utils::IsNone,
    T1: ::rkyv::Archive + ::rkyv::Serialize<S> + ::dbsp::utils::IsNone,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let mut bitmap = [0u64; 1usize];
        let mut ptrs: [Tup2FieldPtr; 2usize] = [Tup2FieldPtr { pos: 0 }; 2usize];
        let mut ptrs_len = 0usize;
        if ::dbsp::utils::IsNone::is_none(&self.0) {
            bitmap[0] |= 1u64 << 0usize;
        } else {
            let pos = serializer.serialize_value(&self.0)?;
            ptrs[ptrs_len] = Tup2FieldPtr { pos };
            ptrs_len += 1;
        }
        if ::dbsp::utils::IsNone::is_none(&self.1) {
            bitmap[0] |= 1u64 << 1usize;
        } else {
            let pos = serializer.serialize_value(&self.1)?;
            ptrs[ptrs_len] = Tup2FieldPtr { pos };
            ptrs_len += 1;
        }
        let ptrs_resolver =
            ::rkyv::vec::ArchivedVec::<::rkyv::rel_ptr::RawRelPtrI32>::serialize_from_slice(
                &ptrs[..ptrs_len],
                serializer,
            )?;
        Ok(Tup2Resolver {
            bitmap,
            ptrs_resolver,
            ptrs_len,
        })
    }
}
impl<D, T0, T1> ::rkyv::Deserialize<Tup2<T0, T1>, D> for ArchivedTup2<T0, T1>
where
    D: ::rkyv::Fallible + ::core::any::Any,
    T0: ::rkyv::Archive + Default,
    T1: ::rkyv::Archive + Default,
    ::rkyv::Archived<T0>: ::rkyv::Deserialize<T0, D>,
    ::rkyv::Archived<T1>: ::rkyv::Deserialize<T1, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup2<T0, T1>, D::Error> {
        let version = (deserializer as &mut dyn ::core::any::Any)
            .downcast_mut::<::dbsp::storage::file::Deserializer>()
            .map(|deserializer| deserializer.version())
            .expect("passed wrong deserializer");
        if version <= 3 {
            let legacy = unsafe { &*(self as *const _ as *const ArchivedTup2V3<T0, T1>) };
            return legacy.deserialize(deserializer);
        }
        let mut ptr_idx = 0usize;
        let t0 = if self.none_bit_set(0) {
            T0::default()
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T0>>()
            };
            ptr_idx += 1;
            archived.deserialize(deserializer)?
        };
        let t1 = if self.none_bit_set(1) {
            T1::default()
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(ptr_idx)
                    .as_ptr()
                    .cast::<::rkyv::Archived<T1>>()
            };
            ptr_idx += 1;
            archived.deserialize(deserializer)?
        };
        Ok(Tup2(t0, t1))
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
        2usize
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
    fn is_none(&self) -> bool {
        false
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
fn rkyv_compact_tup1_option_roundtrip() {
    use feldera_sqllib::SqlString;

    let tup_some = Tup1::new(Some(SqlString::from_ref("hello")));
    let bytes_some = dbsp::storage::file::to_bytes(&tup_some).unwrap();
    let restored_some: Tup1<Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes_some[..]);
    assert_eq!(restored_some, tup_some);

    let tup_none: Tup1<Option<SqlString>> = Tup1::new(None);
    let bytes_none = dbsp::storage::file::to_bytes(&tup_none).unwrap();
    let restored_none: Tup1<Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes_none[..]);
    assert_eq!(restored_none, tup_none);
    assert_eq!(
        bytes_none.len(),
        core::mem::size_of::<ArchivedTup1<Option<SqlString>>>()
    );

    assert!(
        bytes_none.len() < bytes_some.len(),
        "expected `None` to serialize smaller than `Some`, got None={} Some={}",
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
    assert_eq!(archived_t0_some.as_ref().unwrap().as_str(), "hello");
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

    assert_eq!(
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
        bytes_vals.len() > core::mem::size_of::<ArchivedTup2<SqlString, i32>>(),
        "expected serialized bytes to include out-of-line data, got bytes={} archived={}",
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
    assert_eq!(
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

    assert!(
        bytes_none.len() < bytes_11.len(),
        "expected `(None, None)` to serialize smaller than `(Some, Some)`, got None={} Some={}",
        bytes_none.len(),
        bytes_11.len()
    );
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
    assert_eq!(t0_10.as_ref().unwrap().as_str(), "hi");

    let tup_01: Tup2<Option<SqlString>, Option<i32>> = Tup2::new(None, Some(42));
    let bytes_01 = dbsp::storage::file::to_bytes(&tup_01).unwrap();
    let archived_01 =
        unsafe { rkyv::archived_root::<Tup2<Option<SqlString>, Option<i32>>>(&bytes_01[..]) };
    let t0_01 = archived_01.get_t0();
    let t1_01 = archived_01.get_t1();
    assert!(t0_01.is_none());
    assert!(t1_01.is_some());
    assert_eq!(*t1_01.as_ref().unwrap(), 42);
}
