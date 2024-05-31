//! Strongly typed wrappers around dynamically typed batch types.
//!
//! These wrappers are used to implement type-safe wrappers around DBSP
//! operators.

pub use crate::{
    algebra::{
        IndexedZSet as DynIndexedZSet, IndexedZSetReader as DynIndexedZSetReader,
        OrdIndexedZSet as DynOrdIndexedZSet, OrdZSet as DynOrdZSet,
        VecIndexedZSet as DynVecIndexedZSet, VecZSet as DynVecZSet, ZSet as DynZSet,
        ZSetReader as DynZSetReader,
    },
    trace::{
        Batch as DynBatch, BatchReader as DynBatchReader,
        FallbackIndexedWSet as DynFallbackIndexedWSet, FallbackKeyBatch as DynFallbackKeyBatch,
        FallbackValBatch as DynFallbackValBatch, FallbackWSet as DynFallbackWSet,
        FileIndexedWSet as DynFileIndexedWSet, FileKeyBatch as DynFileKeyBatch,
        FileValBatch as DynFileValBatch, FileWSet as DynFileWSet,
        OrdIndexedWSet as DynOrdIndexedWSet, OrdKeyBatch as DynOrdKeyBatch,
        OrdValBatch as DynOrdValBatch, OrdWSet as DynOrdWSet, Spillable as DynSpillable,
        Spine as DynSpine, Stored as DynStored, Trace as DynTrace,
        VecIndexedWSet as DynVecIndexedWSet, VecKeyBatch as DynVecKeyBatch,
        VecValBatch as DynVecValBatch, VecWSet as DynVecWSet,
    },
    DBData, DBWeight, DynZWeight, Stream, Timestamp, ZWeight,
};
use crate::{
    dynamic::{DataTrait, DynData, DynUnit, Erase, LeanVec, WeightTrait},
    trace::BatchReaderFactories,
    Circuit,
};
use dyn_clone::clone_box;
use size_of::SizeOf;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut, Neg},
};

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    dynamic::DowncastTrait,
    utils::Tup2,
    NumEntries,
};

/// A strongly typed wrapper around [`DynBatchReader`].
pub trait BatchReader: 'static {
    type Inner: DynBatchReader<
        Time = Self::Time,
        Key = Self::DynK,
        Val = Self::DynV,
        R = Self::DynR,
    >;

    /// Concrete key type.
    type Key: DBData + Erase<Self::DynK>;

    /// Concrete value type.
    type Val: DBData + Erase<Self::DynV>;

    /// Concrete weight typ.
    type R: DBWeight + Erase<Self::DynR>;

    /// Dynamic key type (e.g., [`DynData`]).
    type DynK: DataTrait + ?Sized;

    /// Dynamic value typ, (e.g., [`DynData`]).
    type DynV: DataTrait + ?Sized;

    /// Dynamic weight type (e.g., [`DynZWeight`]).
    type DynR: WeightTrait + ?Sized;

    type Time: Timestamp;

    /// Factories for `Self::Inner`.
    fn factories() -> <Self::Inner as DynBatchReader>::Factories {
        BatchReaderFactories::new::<Self::Key, Self::Val, Self::R>()
    }

    /// Extract the dynamically typed batch.
    fn inner(&self) -> &Self::Inner;

    fn inner_mut(&mut self) -> &mut Self::Inner;

    /// Drop the statically typed wrapper and return the inner dynamic batch type.
    fn into_inner(self) -> Self::Inner;

    /// Create a statically typed wrapper around `inner`.
    fn from_inner(inner: Self::Inner) -> Self;

    /// Convert a stream of batches of statically typed batches of type `Self` into a stream
    /// of dynamically typed batches `Self::Inner` batches.
    ///
    /// This operation has not runtime cost.
    fn stream_inner<C: Clone>(stream: &Stream<C, Self>) -> Stream<C, Self::Inner>
    where
        Self: Sized;

    /// Convert a stream of dynamically typed batches of type `Self::Inner` into a stream
    /// of statically typed batches of type `Self`.
    fn stream_from_inner<C: Clone>(stream: &Stream<C, Self::Inner>) -> Stream<C, Self>
    where
        Self: Sized;
}

pub trait Spillable: Batch<Time = ()>
where
    Self::InnerBatch: DynSpillable,
{
}

impl<B> Spillable for B
where
    B: Batch<Time = ()>,
    B::InnerBatch: DynSpillable,
{
}

pub trait Stored: Batch<Time = ()>
where
    Self::InnerBatch: DynStored,
{
}

impl<B> Stored for B
where
    B: Batch<Time = ()>,
    B::InnerBatch: DynStored,
{
}

/// A statically typed wrapper around [`DynBatch`].
pub trait Batch: BatchReader<Inner = Self::InnerBatch> + Clone {
    type InnerBatch: DynBatch<Time = Self::Time, Key = Self::DynK, Val = Self::DynV, R = Self::DynR>;
}

impl<B> Batch for B
where
    B: BatchReader + Clone,
    B::Inner: DynBatch,
{
    type InnerBatch = B::Inner;
}

/// A statically typed wrapper around [`DynTrace`].
pub trait Trace: BatchReader<Inner = Self::InnerTrace> {
    type InnerTrace: DynTrace<Time = Self::Time, Key = Self::DynK, Val = Self::DynV, R = Self::DynR>;
}

impl<T> Trace for T
where
    T: BatchReader,
    T::Inner: DynTrace<Time = T::Time, Key = T::DynK, Val = T::DynV, R = T::DynR>,
{
    type InnerTrace = <T as BatchReader>::Inner;
}

/// A statically typed wrapper around [`DynIndexedZSetReader`].
pub trait IndexedZSetReader: BatchReader<R = ZWeight, DynR = DynZWeight, Time = ()> {}

impl<Z> IndexedZSetReader for Z where Z: BatchReader<R = ZWeight, DynR = DynZWeight, Time = ()> {}

/// A statically typed wrapper around [`DynIndexedZSet`].
pub trait IndexedZSet:
    Batch<R = ZWeight, DynR = DynZWeight, Time = (), InnerBatch = Self::InnerIndexedZSet>
{
    type InnerIndexedZSet: DynIndexedZSet<
        Time = Self::Time,
        Key = Self::DynK,
        Val = Self::DynV,
        R = Self::DynR,
    >;
}

impl<Z> IndexedZSet for Z
where
    Z: Batch<R = ZWeight, DynR = DynZWeight, Time = ()>,
    Z::InnerBatch: DynIndexedZSet,
{
    type InnerIndexedZSet = Z::InnerBatch;
}

/// A statically typed wrapper around [`DynZSetReader`].
pub trait ZSetReader: IndexedZSetReader<Val = ()> {}

impl<Z> ZSetReader for Z where Z: IndexedZSetReader<Val = ()> {}

/// A statically typed wrapper around [`DynZSet`].
pub trait ZSet: IndexedZSet<Val = (), DynV = DynUnit> {
    fn weighted_count(&self) -> ZWeight;
}

impl<Z> ZSet for Z
where
    Z: IndexedZSet<Val = (), DynV = DynUnit>,
{
    fn weighted_count(&self) -> ZWeight {
        let mut w = 0;
        self.inner().weighted_count(w.erase_mut());
        w
    }
}

/// A statically typed wrapper around a dynamically typed batch `B` with concrete key, value, and
/// weight types `K`, `V`, and `R` respectively.
#[derive(Debug, Clone, Eq, SizeOf)]
// repr(transparent) guarantees that we can safely transmute this to `inner`.
#[repr(transparent)]
pub struct TypedBatch<K, V, R, B> {
    inner: B,
    phantom: PhantomData<fn(&K, &V, &R)>,
}

impl<K, V, R, B, B2> PartialEq<TypedBatch<K, V, R, B2>> for TypedBatch<K, V, R, B>
where
    B: PartialEq<B2>,
{
    fn eq(&self, other: &TypedBatch<K, V, R, B2>) -> bool {
        self.inner.eq(&other.inner)
    }
}

// FIXME: this is needed so that batches can be wrapped in Arc and shared
// between threads, e.g., in the `adapters` crate, which can send output batches
// to multiple transport endpoints.  A proper solution is to add the `Sync`
// bound to DBData and propagate it through all layers, so that `TypedBatch`
// is truly `Sync`.
unsafe impl<K, V, R, B> Sync for TypedBatch<K, V, R, B> where B: DynBatch {}
unsafe impl<K, V, R, B> Send for TypedBatch<K, V, R, B> where B: DynBatch {}

impl<K, V, R, B> Default for TypedBatch<K, V, R, B>
where
    B: DynBatch,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    fn default() -> Self {
        Self::new(B::dyn_empty(
            &BatchReaderFactories::new::<K, V, R>(),
            B::Time::default(),
        ))
    }
}

impl<K, V, R, B> Deref for TypedBatch<K, V, R, B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, R, B> HasZero for TypedBatch<K, V, R, B>
where
    B: DynBatch<Time = ()>,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    fn zero() -> Self {
        Self::new(B::dyn_empty(&BatchReaderFactories::new::<K, V, R>(), ()))
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V, R, B> Neg for TypedBatch<K, V, R, B>
where
    B: DynBatchReader + Neg<Output = B>,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self::new(self.inner.neg())
    }
}

impl<K, V, R, B> NegByRef for TypedBatch<K, V, R, B>
where
    B: DynBatchReader + NegByRef,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    fn neg_by_ref(&self) -> Self {
        Self::new(self.inner().neg_by_ref())
    }
}

impl<K, V, R, B> AddByRef for TypedBatch<K, V, R, B>
where
    B: DynBatchReader + AddByRef,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        Self::new(self.inner.add_by_ref(other.inner()))
    }
}

impl<K, V, R, B> AddAssignByRef for TypedBatch<K, V, R, B>
where
    B: DynBatchReader + AddAssignByRef,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.inner.add_assign_by_ref(other.inner())
    }
}

impl<K, V, R, B> NumEntries for TypedBatch<K, V, R, B>
where
    B: DynBatchReader,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    const CONST_NUM_ENTRIES: Option<usize> = B::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.inner.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.inner.num_entries_deep()
    }
}

impl<K, V, R, B> DerefMut for TypedBatch<K, V, R, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V, R, B> TypedBatch<K, V, R, B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            phantom: PhantomData,
        }
    }
}

impl<K, V, R, B> TypedBatch<K, V, R, B>
where
    B: DynBatch,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    /// Create an empty batch.
    pub fn empty(time: B::Time) -> Self {
        Self::new(B::dyn_empty(&BatchReaderFactories::new::<K, V, R>(), time))
    }

    /// Build a batch out of `tuples`.
    pub fn from_tuples(time: B::Time, tuples: Vec<Tup2<Tup2<K, V>, R>>) -> Self {
        Self::new(B::dyn_from_tuples(
            &BatchReaderFactories::new::<K, V, R>(),
            time,
            &mut Box::new(LeanVec::from(tuples)).erase_box(),
        ))
    }

    /// Merge `self` with `other`.
    pub fn merge(&self, other: &Self) -> Self {
        Self::new(self.inner.merge(&other.inner))
    }
}

impl<K, R, B> TypedBatch<K, (), R, B>
where
    B: DynBatch,
    K: DBData + Erase<B::Key>,
    (): Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    pub fn from_keys(time: B::Time, tuples: Vec<Tup2<K, R>>) -> Self {
        Self::from_tuples(
            time,
            tuples
                .into_iter()
                .map(|Tup2(k, r)| Tup2(Tup2(k, ()), r))
                .collect::<Vec<_>>(),
        )
    }
}

impl<K, V, B: DynIndexedZSet> TypedBatch<K, V, ZWeight, B>
where
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
{
    pub fn iter(&self) -> impl Iterator<Item = (K, V, ZWeight)> + '_ {
        self.inner.iter().map(|(boxk, boxv, w)| unsafe {
            (
                boxk.as_ref().downcast::<K>().clone(),
                boxv.as_ref().downcast::<V>().clone(),
                w,
            )
        })
    }
}

impl<K, V, R, B> BatchReader for TypedBatch<K, V, R, B>
where
    B: DynBatchReader,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    type Inner = B;
    type Key = K;
    type Val = V;
    type R = R;
    type Time = B::Time;
    type DynK = B::Key;
    type DynV = B::Val;
    type DynR = B::R;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn into_inner(self) -> Self::Inner {
        self.inner
    }

    fn from_inner(inner: Self::Inner) -> Self {
        Self {
            inner,
            phantom: PhantomData,
        }
    }

    fn stream_inner<C: Clone>(stream: &Stream<C, Self>) -> Stream<C, B> {
        // Safety: repr(transparent) on TypedBatch guarantees that this is
        // safe.
        unsafe { stream.transmute_payload() }
    }

    fn stream_from_inner<C: Clone>(stream: &Stream<C, Self::Inner>) -> Stream<C, Self> {
        // Safety: repr(transparent) on TypedBatch guarantees that this is
        // safe.
        unsafe { stream.transmute_payload() }
    }
}

pub type OrdWSet<K, R, DynR> = TypedBatch<K, (), R, DynOrdWSet<DynData, DynR>>;
pub type OrdZSet<K> = TypedBatch<K, (), ZWeight, DynOrdZSet<DynData>>;
pub type OrdIndexedWSet<K, V, R, DynR> =
    TypedBatch<K, V, R, DynOrdIndexedWSet<DynData, DynData, DynR>>;
pub type OrdIndexedZSet<K, V> = TypedBatch<K, V, ZWeight, DynOrdIndexedZSet<DynData, DynData>>;
pub type OrdKeyBatch<K, T, R, DynR> = TypedBatch<K, (), R, DynOrdKeyBatch<DynData, T, DynR>>;
pub type OrdValBatch<K, V, T, R, DynR> =
    TypedBatch<K, V, R, DynOrdValBatch<DynData, DynData, T, DynR>>;

pub type VecWSet<K, R, DynR> = TypedBatch<K, (), R, DynVecWSet<DynData, DynR>>;
pub type VecZSet<K> = TypedBatch<K, (), ZWeight, DynVecZSet<DynData>>;
pub type VecIndexedWSet<K, V, R, DynR> =
    TypedBatch<K, V, R, DynVecIndexedWSet<DynData, DynData, DynR>>;
pub type VecIndexedZSet<K, V> = TypedBatch<K, V, ZWeight, DynVecIndexedZSet<DynData, DynData>>;
pub type VecKeyBatch<K, T, R, DynR> = TypedBatch<K, (), R, DynVecKeyBatch<DynData, T, DynR>>;
pub type VecValBatch<K, V, T, R, DynR> =
    TypedBatch<K, V, R, DynVecValBatch<DynData, DynData, T, DynR>>;

pub type FileWSet<K, R, DynR> = TypedBatch<K, (), R, DynFileWSet<DynData, DynR>>;
pub type FileZSet<K> = TypedBatch<K, (), ZWeight, DynFileWSet<DynData, DynZWeight>>;
pub type FileIndexedWSet<K, V, R, DynR> =
    TypedBatch<K, V, R, DynFileIndexedWSet<DynData, DynData, DynR>>;
pub type FileIndexedZSet<K, V> =
    TypedBatch<K, V, ZWeight, DynFileIndexedWSet<DynData, DynData, DynZWeight>>;
pub type FileKeyBatch<K, T, R, DynR> = TypedBatch<K, (), R, DynFileKeyBatch<DynData, T, DynR>>;
pub type FileValBatch<K, V, T, R, DynR> =
    TypedBatch<K, V, R, DynFileValBatch<DynData, DynData, T, DynR>>;

pub type FallbackWSet<K, R, DynR> = TypedBatch<K, (), R, DynFallbackWSet<DynData, DynR>>;
pub type FallbackZSet<K> = TypedBatch<K, (), ZWeight, DynFallbackWSet<DynData, DynZWeight>>;
pub type FallbackIndexedWSet<K, V, R, DynR> =
    TypedBatch<K, V, R, DynFallbackIndexedWSet<DynData, DynData, DynR>>;
pub type FallbackIndexedZSet<K, V> =
    TypedBatch<K, V, ZWeight, DynFallbackIndexedWSet<DynData, DynData, DynZWeight>>;
pub type FallbackKeyBatch<K, T, R, DynR> =
    TypedBatch<K, (), R, DynFallbackKeyBatch<DynData, T, DynR>>;
pub type FallbackValBatch<K, V, T, R, DynR> =
    TypedBatch<K, V, R, DynFallbackValBatch<DynData, DynData, T, DynR>>;

pub type Spine<B> = TypedBatch<
    <B as BatchReader>::Key,
    <B as BatchReader>::Val,
    <B as BatchReader>::R,
    DynSpine<<B as BatchReader>::Inner>,
>;
pub type SpilledSpine<B> = TypedBatch<
    <B as BatchReader>::Key,
    <B as BatchReader>::Val,
    <B as BatchReader>::R,
    DynSpine<<<B as Batch>::InnerBatch as DynSpillable>::Spilled>,
>;

pub type SpilledBatch<B> = TypedBatch<
    <B as BatchReader>::Key,
    <B as BatchReader>::Val,
    <B as BatchReader>::R,
    <<B as Batch>::InnerBatch as DynSpillable>::Spilled,
>;

impl<C: Clone, B: BatchReader> Stream<C, B> {
    pub fn inner(&self) -> Stream<C, B::Inner> {
        BatchReader::stream_inner(self)
    }
}

impl<C: Clone, B: DynBatchReader> Stream<C, B> {
    pub fn typed<TB>(&self) -> Stream<C, TB>
    where
        TB: BatchReader<Inner = B>,
    {
        // Safety: repr(transparent) on TypedBatch guarantees that this is
        // safe.
        unsafe { self.transmute_payload() }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, SizeOf)]
#[repr(transparent)]
pub struct TypedBox<T, D: ?Sized> {
    inner: Box<D>,
    phantom: PhantomData<fn(&T)>,
}

impl<T, D> Deref for TypedBox<T, D>
where
    D: DataTrait + ?Sized,
    T: DBData + Erase<D>,
{
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { self.inner.downcast() }
    }
}

impl<T, D> DerefMut for TypedBox<T, D>
where
    D: DataTrait + ?Sized,
    T: DBData + Erase<D>,
{
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.inner.downcast_mut() }
    }
}

impl<T, D: ?Sized> NumEntries for TypedBox<T, D> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        1
    }

    fn num_entries_deep(&self) -> usize {
        1
    }
}

impl<T, D: DataTrait + ?Sized> TypedBox<T, D> {
    pub fn new(v: T) -> Self
    where
        T: DBData + Erase<D>,
    {
        Self {
            inner: Box::new(v).erase_box(),
            phantom: PhantomData,
        }
    }

    pub fn inner(&self) -> &D {
        self.inner.as_ref()
    }

    pub fn into_inner(self) -> Box<D> {
        self.inner
    }
}

impl<T, D> Clone for TypedBox<T, D>
where
    D: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            inner: clone_box(self.inner.as_ref()),
            phantom: PhantomData,
        }
    }
}

impl<C: Clone, D: DataTrait + ?Sized> Stream<C, Box<D>> {
    /// Adds type information to `self`, wrapping each element
    /// in a [`TypedBox`].  This function is a noop at runtime.
    ///
    /// # Safety
    ///
    /// `self` must contain concrete values of type `T`.
    pub unsafe fn typed_data<T>(&self) -> Stream<C, TypedBox<T, D>>
    where
        T: DBData + Erase<D>,
    {
        self.transmute_payload()
    }
}

impl<C: Circuit, T, D: ?Sized> Stream<C, TypedBox<T, D>> {
    pub fn inner_data(&self) -> Stream<C, Box<D>> {
        unsafe { self.transmute_payload() }
    }
}

impl<C: Circuit, T: DBData, D: DataTrait + ?Sized> Stream<C, TypedBox<T, D>> {
    pub fn inner_typed(&self) -> Stream<C, T> {
        self.apply(|typed_box| unsafe { typed_box.inner().downcast::<T>().clone() })
    }
}
