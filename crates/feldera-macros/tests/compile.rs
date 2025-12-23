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
    IsNone,
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
impl<T0, W> dbsp::algebra::MulByRef<W> for Tup1<T0>
where
    T0: dbsp::algebra::MulByRef<W, Output = T0>,
    W: dbsp::algebra::ZRingValue,
{
    type Output = Self;
    fn mul_by_ref(&self, other: &W) -> Self::Output {
        let Tup1(t0) = self;
        Tup1(t0.mul_by_ref(other))
    }
}
impl<T0> dbsp::algebra::HasZero for Tup1<T0>
where
    T0: dbsp::algebra::HasZero,
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
impl<T0> dbsp::algebra::AddByRef for Tup1<T0>
where
    T0: dbsp::algebra::AddByRef,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        let Tup1(t0) = self;
        let Tup1(other_t0) = other;
        Tup1(t0.add_by_ref(other_t0))
    }
}
impl<T0> dbsp::algebra::AddAssignByRef for Tup1<T0>
where
    T0: dbsp::algebra::AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        let Tup1(ref mut t0) = self;
        let Tup1(ref other_t0) = other;
        t0.add_assign_by_ref(other_t0);
    }
}
impl<T0> dbsp::algebra::NegByRef for Tup1<T0>
where
    T0: dbsp::algebra::NegByRef,
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
impl<T0> dbsp::NumEntries for Tup1<T0>
where
    T0: dbsp::NumEntries,
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
        f.debug_tuple(stringify!(Tup1)).field(&t0).finish()
    }
}
impl<T0: Copy> Copy for Tup1<T0> {}

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
    IsNone,
)]
pub struct Tup2<T0, T1>(pub T0, pub T1);
impl<T0, T1> Tup2<T0, T1> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(t0: T0, t1: T1) -> Self {
        Self(t0, t1)
    }
}
impl<T0: core::fmt::Debug, T1: core::fmt::Debug> core::fmt::Debug for Tup2<T0, T1> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        let Tup2(t0, t1) = self;
        f.debug_tuple("").field(&t0).field(&t1).finish()
    }
}
impl<T0: Copy, T1: Copy> Copy for Tup2<T0, T1> {}

// ------------------------------------------------------------------------------------------------
// Compact `rkyv` format for `Tup1<T0>`
//
// Motivation: `Archived<Option<T>>` is always `size_of::<Archived<T>>()` even when the option is
// `None`. For very wide tuples (`Tup370`, ...), a large fraction of `None`s can otherwise dominate
// checkpoint size. This format stores a bitmap and only serializes values whose bit is *not* set
// according to `IsNone`.
// ------------------------------------------------------------------------------------------------

#[repr(C)]
pub struct ArchivedTup1<T0>
where
    T0: rkyv::Archive,
{
    bitmap: u8,
    ptrs: rkyv::vec::ArchivedVec<rkyv::RawRelPtr>,
    _phantom: core::marker::PhantomData<fn() -> T0>,
}

#[inline]
unsafe fn archived_none_ptr<T: rkyv::Archive>() -> *const Archived<T> {
    static NONE: OnceLock<usize> = OnceLock::new();
    let ptr = *NONE.get_or_init(|| {
        // This keeps a per-type, properly aligned `Archived<T>` that we can
        // reuse when the bitmap indicates a `None` value.
        // SAFETY: This is only valid when `Archived<T>` is `ArchivedOption<...>`.
        let boxed: Box<Archived<T>> = Box::new(unsafe { core::mem::zeroed() });
        Box::into_raw(boxed) as usize
    });
    ptr as *const Archived<T>
}

impl<T0> ArchivedTup1<T0>
where
    T0: rkyv::Archive,
{
    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < 8);
        (self.bitmap & (1u8 << idx)) != 0
    }

    #[inline]
    fn get_t0(&self) -> &Archived<T0> {
        if self.none_bit_set(0) {
            // SAFETY: The bitmap only marks `None` for types whose archived
            // representation is `ArchivedOption<...>`; a zeroed archived
            // option is valid for the `None` variant.
            unsafe { &*archived_none_ptr::<T0>() }
        } else {
            debug_assert!(!self.ptrs.is_empty());
            // SAFETY: `ptrs[0]` points at the archived `T0` when the bit is clear.
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(0)
                    .as_ptr()
                    .cast::<Archived<T0>>()
            }
        }
    }
}

impl<T0> core::cmp::PartialEq for ArchivedTup1<T0>
where
    T0: rkyv::Archive,
    Archived<T0>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.get_t0() == other.get_t0()
    }
}

impl<T0> core::cmp::Eq for ArchivedTup1<T0>
where
    T0: rkyv::Archive,
    Archived<T0>: core::cmp::Eq,
{
}

impl<T0> core::cmp::PartialOrd for ArchivedTup1<T0>
where
    T0: rkyv::Archive,
    Archived<T0>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T0> core::cmp::Ord for ArchivedTup1<T0>
where
    T0: rkyv::Archive,
    Archived<T0>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.get_t0().cmp(other.get_t0())
    }
}

pub struct Tup1Resolver {
    bitmap: u8,
    ptrs_resolver: rkyv::vec::VecResolver,
    ptrs_len: usize,
}

struct FieldPtr {
    pos: usize,
}

impl rkyv::Archive for FieldPtr {
    type Archived = rkyv::RawRelPtr;
    type Resolver = usize;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        rkyv::RawRelPtr::emplace(pos, resolver, out);
    }
}

impl<S> rkyv::Serialize<S> for FieldPtr
where
    S: rkyv::ser::Serializer + ?Sized,
{
    #[inline]
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(self.pos)
    }
}

impl<T0> rkyv::Archive for Tup1<T0>
where
    T0: rkyv::Archive,
{
    type Archived = ArchivedTup1<T0>;
    type Resolver = Tup1Resolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = out_field!(out.bitmap);
        u8::resolve(&resolver.bitmap, pos + fp, (), fo);

        let (fp, fo) = out_field!(out.ptrs);
        let vec_pos = pos + fp;
        rkyv::vec::ArchivedVec::<rkyv::RawRelPtr>::resolve_from_len(
            resolver.ptrs_len,
            vec_pos,
            resolver.ptrs_resolver,
            fo,
        );
    }
}

impl<S, T0> rkyv::Serialize<S> for Tup1<T0>
where
    S: rkyv::ser::Serializer + rkyv::ser::ScratchSpace + ?Sized,
    T0: rkyv::Archive + rkyv::Serialize<S> + IsNone,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        if self.0.is_none() {
            Ok(Tup1Resolver {
                bitmap: 1,
                ptrs_resolver: rkyv::vec::ArchivedVec::<rkyv::RawRelPtr>::serialize_from_slice(
                    &[] as &[FieldPtr],
                    serializer,
                )?,
                ptrs_len: 0,
            })
        } else {
            let t0_pos = serializer.serialize_value(&self.0)?;
            let ptrs = [FieldPtr { pos: t0_pos }];
            let ptrs_resolver =
                rkyv::vec::ArchivedVec::<rkyv::RawRelPtr>::serialize_from_slice(&ptrs, serializer)?;
            Ok(Tup1Resolver {
                bitmap: 0,
                ptrs_resolver,
                ptrs_len: 1,
            })
        }
    }
}

impl<D, T0> rkyv::Deserialize<Tup1<T0>, D> for ArchivedTup1<T0>
where
    D: rkyv::Fallible + ?Sized,
    T0: rkyv::Archive + Default,
    Archived<T0>: rkyv::Deserialize<T0, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup1<T0>, D::Error> {
        if self.none_bit_set(0) {
            Ok(Tup1(T0::default()))
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(0)
                    .as_ptr()
                    .cast::<Archived<T0>>()
            };
            Ok(Tup1(archived.deserialize(deserializer)?))
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Compact `rkyv` format for `Tup2<T0, T1>`
//
// Uses `ArchivedVec<RawRelPtr>` so the pointer list can be empty when all fields are `None`
// according to `IsNone`.
// ------------------------------------------------------------------------------------------------

#[repr(C)]
pub struct ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
{
    bitmap: u8,
    ptrs: rkyv::vec::ArchivedVec<rkyv::RawRelPtr>,
    _phantom: core::marker::PhantomData<fn() -> (T0, T1)>,
}

impl<T0, T1> ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
{
    #[inline]
    fn ptr_index(&self, field_idx: usize) -> usize {
        debug_assert!(field_idx <= 1);
        match field_idx {
            0 => 0,
            _ => {
                if self.none_bit_set(0) {
                    0
                } else {
                    1
                }
            }
        }
    }

    #[inline]
    fn none_bit_set(&self, idx: usize) -> bool {
        debug_assert!(idx < 8);
        (self.bitmap & (1u8 << idx)) != 0
    }

    #[inline]
    fn get_t0(&self) -> &Archived<T0> {
        if self.none_bit_set(0) {
            // SAFETY: See `ArchivedTup1::get_t0` for the `ArchivedOption` reasoning.
            unsafe { &*archived_none_ptr::<T0>() }
        } else {
            debug_assert!(!self.ptrs.is_empty());
            // SAFETY: `ptrs[0]` points at the archived `T0` when the bit is clear.
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(0)
                    .as_ptr()
                    .cast::<Archived<T0>>()
            }
        }
    }

    #[inline]
    fn get_t1(&self) -> &Archived<T1> {
        if self.none_bit_set(1) {
            // SAFETY: See `ArchivedTup1::get_t0` for the `ArchivedOption` reasoning.
            unsafe { &*archived_none_ptr::<T1>() }
        } else {
            let idx = self.ptr_index(1);
            debug_assert!(idx < self.ptrs.len());
            // SAFETY: `ptrs[idx]` points at the archived `T1` when its bit is clear.
            unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(idx)
                    .as_ptr()
                    .cast::<Archived<T1>>()
            }
        }
    }
}

impl<T0, T1> core::cmp::PartialEq for ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
    Archived<T0>: core::cmp::PartialEq,
    Archived<T1>: core::cmp::PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.get_t0() == other.get_t0() && self.get_t1() == other.get_t1() }
    }
}

impl<T0, T1> core::cmp::Eq for ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
    Archived<T0>: core::cmp::Eq,
    Archived<T1>: core::cmp::Eq,
{
}

impl<T0, T1> core::cmp::PartialOrd for ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
    Archived<T0>: core::cmp::Ord,
    Archived<T1>: core::cmp::Ord,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T0, T1> core::cmp::Ord for ArchivedTup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
    Archived<T0>: core::cmp::Ord,
    Archived<T1>: core::cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let t0_cmp = unsafe { self.get_t0().cmp(other.get_t0()) };
        if t0_cmp != core::cmp::Ordering::Equal {
            return t0_cmp;
        }
        unsafe { self.get_t1().cmp(other.get_t1()) }
    }
}

pub struct Tup2Resolver {
    bitmap: u8,
    ptrs_resolver: rkyv::vec::VecResolver,
    ptrs_len: usize,
}

impl<T0, T1> rkyv::Archive for Tup2<T0, T1>
where
    T0: rkyv::Archive,
    T1: rkyv::Archive,
{
    type Archived = ArchivedTup2<T0, T1>;
    type Resolver = Tup2Resolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = out_field!(out.bitmap);
        u8::resolve(&resolver.bitmap, pos + fp, (), fo);

        let (fp, fo) = out_field!(out.ptrs);
        let vec_pos = pos + fp;
        rkyv::vec::ArchivedVec::<rkyv::RawRelPtr>::resolve_from_len(
            resolver.ptrs_len,
            vec_pos,
            resolver.ptrs_resolver,
            fo,
        );

        let (_fp, fo) = out_field!(out._phantom);
        fo.write(core::marker::PhantomData);
    }
}

impl<S, T0, T1> rkyv::Serialize<S> for Tup2<T0, T1>
where
    S: rkyv::ser::Serializer + rkyv::ser::ScratchSpace + ?Sized,
    T0: rkyv::Archive + rkyv::Serialize<S> + IsNone,
    T1: rkyv::Archive + rkyv::Serialize<S> + IsNone,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let mut bitmap = 0u8;
        let mut ptrs = [FieldPtr { pos: 0 }, FieldPtr { pos: 0 }];
        let mut ptrs_len = 0usize;

        if self.0.is_none() {
            bitmap |= 1;
        } else {
            let pos = serializer.serialize_value(&self.0)?;
            ptrs[ptrs_len] = FieldPtr { pos };
            ptrs_len += 1;
        }

        if self.1.is_none() {
            bitmap |= 2;
        } else {
            let pos = serializer.serialize_value(&self.1)?;
            ptrs[ptrs_len] = FieldPtr { pos };
            ptrs_len += 1;
        }

        let ptrs_resolver = rkyv::vec::ArchivedVec::<rkyv::RawRelPtr>::serialize_from_slice(
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

impl<D, T0, T1> rkyv::Deserialize<Tup2<T0, T1>, D> for ArchivedTup2<T0, T1>
where
    D: rkyv::Fallible + ?Sized,
    T0: rkyv::Archive + Default,
    T1: rkyv::Archive + Default,
    Archived<T0>: rkyv::Deserialize<T0, D>,
    Archived<T1>: rkyv::Deserialize<T1, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<Tup2<T0, T1>, D::Error> {
        let mut idx = 0usize;

        let t0 = if self.none_bit_set(0) {
            T0::default()
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(idx)
                    .as_ptr()
                    .cast::<Archived<T0>>()
            };
            idx += 1;
            archived.deserialize(deserializer)?
        };

        let t1 = if self.none_bit_set(1) {
            T1::default()
        } else {
            let archived = unsafe {
                &*self
                    .ptrs
                    .as_slice()
                    .get_unchecked(idx)
                    .as_ptr()
                    .cast::<Archived<T1>>()
            };
            idx += 1;
            archived.deserialize(deserializer)?
        };

        debug_assert_eq!(idx, self.ptrs.len());
        Ok(Tup2(t0, t1))
    }
}

impl<T0> dbsp::circuit::checkpointer::Checkpoint for Tup1<T0>
where
    Tup1<T0>:
        ::rkyv::Serialize<dbsp::storage::file::Serializer> + dbsp::storage::file::Deserializable,
{
    fn checkpoint(&self) -> Result<Vec<u8>, dbsp::Error> {
        let mut s = dbsp::storage::file::Serializer::default();
        let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
        let data = s.into_serializer().into_inner().into_vec();
        Ok(data)
    }
    fn restore(&mut self, data: &[u8]) -> Result<(), dbsp::Error> {
        *self = dbsp::trace::unaligned_deserialize(data);
        Ok(())
    }
}

impl<T0, T1> dbsp::circuit::checkpointer::Checkpoint for Tup2<T0, T1>
where
    Tup2<T0, T1>:
        ::rkyv::Serialize<dbsp::storage::file::Serializer> + dbsp::storage::file::Deserializable,
{
    fn checkpoint(&self) -> Result<Vec<u8>, dbsp::Error> {
        let mut s = dbsp::storage::file::Serializer::default();
        let _offset = ::rkyv::ser::Serializer::serialize_value(&mut s, self).unwrap();
        let data = s.into_serializer().into_inner().into_vec();
        Ok(data)
    }
    fn restore(&mut self, data: &[u8]) -> Result<(), dbsp::Error> {
        *self = dbsp::trace::unaligned_deserialize(data);
        Ok(())
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
