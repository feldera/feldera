//! Prebuilt native archive adapters for two accumulator layouts.

use dbsp::{
    algebra::{AddAssignByRef, AddByRef, HasZero, MulByRef},
    dynamic::{HashRepr, OrdRepr},
    utils::{Tup2, Tup3},
};
use feldera_native_probe::{
    ArchiveField, ArchiveInteger, ArchiveLayout, ArchivedBatch, CompiledArchive,
};
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    mem::{offset_of, size_of},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

type Sum = Tup3<i64, i64, i64>;
type Count = Tup2<Tup2<i128, i64>, Tup2<i64, i64>>;
pub(crate) type NativeSum = NativeWeight<Sum>;
pub(crate) type NativeCount = NativeWeight<Count>;

#[derive(Default)]
pub(crate) struct ArchiveCounts {
    writes: AtomicUsize,
    reads: AtomicUsize,
}

/// Each implementation describes the complete, fixed-width archived value.
///
/// # Safety
///
/// `encode` must return one fully initialized row with the layout and contents
/// of `Self::Archived`. The row must be position-independent because the
/// archive resolver copies it into the serializer's output buffer.
pub(crate) unsafe trait FixedWeight: Archive {
    fn encoder() -> Arc<CompiledArchive>;
    fn encode(&self, encoder: &CompiledArchive) -> ArchivedBatch;
    fn counters() -> &'static ArchiveCounts;
}

type EncoderCache = OnceLock<Mutex<Weak<CompiledArchive>>>;

fn shared_encoder(
    cache: &EncoderCache,
    layout: impl FnOnce() -> ArchiveLayout,
) -> Arc<CompiledArchive> {
    let mut cached = cache.get_or_init(Mutex::default).lock().unwrap();
    if let Some(encoder) = cached.upgrade() {
        return encoder;
    }
    let encoder = Arc::new(CompiledArchive::compile(layout()).unwrap());
    *cached = Arc::downgrade(&encoder);
    encoder
}

fn required(integer: ArchiveInteger, payload_offset: usize) -> ArchiveField {
    ArchiveField {
        integer,
        payload_offset,
        null_tag_offset: None,
    }
}

// SAFETY: The archive contains only three signed 64-bit integers. The layout
// uses the reference archive's size and field offsets, including its padding.
unsafe impl FixedWeight for Sum {
    fn counters() -> &'static ArchiveCounts {
        static COUNTERS: OnceLock<ArchiveCounts> = OnceLock::new();
        COUNTERS.get_or_init(ArchiveCounts::default)
    }

    fn encoder() -> Arc<CompiledArchive> {
        static CACHE: EncoderCache = OnceLock::new();
        shared_encoder(&CACHE, || {
            type Wire = Archived<Sum>;
            ArchiveLayout::new(
                size_of::<Wire>(),
                vec![
                    required(ArchiveInteger::I64, offset_of!(Wire, 0)),
                    required(ArchiveInteger::I64, offset_of!(Wire, 1)),
                    required(ArchiveInteger::I64, offset_of!(Wire, 2)),
                ],
            )
            .unwrap()
        })
    }

    fn encode(&self, encoder: &CompiledArchive) -> ArchivedBatch {
        let Tup3(sum, count, presence) = *self;
        encoder
            .encode_wide(&[vec![
                Some(sum.into()),
                Some(count.into()),
                Some(presence.into()),
            ]])
            .unwrap()
    }
}

// SAFETY: The nested tuples archive one signed 128-bit integer and three
// signed 64-bit integers. All offsets and the size come from the reference.
unsafe impl FixedWeight for Count {
    fn counters() -> &'static ArchiveCounts {
        static COUNTERS: OnceLock<ArchiveCounts> = OnceLock::new();
        COUNTERS.get_or_init(ArchiveCounts::default)
    }

    fn encoder() -> Arc<CompiledArchive> {
        static CACHE: EncoderCache = OnceLock::new();
        shared_encoder(&CACHE, || {
            type Wire = Archived<Count>;
            ArchiveLayout::new(
                size_of::<Wire>(),
                vec![
                    required(ArchiveInteger::I128, offset_of!(Wire, 0.0)),
                    required(ArchiveInteger::I64, offset_of!(Wire, 0.1)),
                    required(ArchiveInteger::I64, offset_of!(Wire, 1.0)),
                    required(ArchiveInteger::I64, offset_of!(Wire, 1.1)),
                ],
            )
            .unwrap()
        })
    }

    fn encode(&self, encoder: &CompiledArchive) -> ArchivedBatch {
        let Tup2(Tup2(rows, presence), Tup2(distinct, distinct_presence)) = *self;
        encoder
            .encode_wide(&[vec![
                Some(rows),
                Some(presence.into()),
                Some(distinct.into()),
                Some(distinct_presence.into()),
            ]])
            .unwrap()
    }
}

#[derive(Clone)]
pub(crate) struct NativeWeight<T> {
    value: T,
    encoder: Arc<CompiledArchive>,
}

impl<T: FixedWeight> NativeWeight<T> {
    pub(crate) fn encoder() -> Arc<CompiledArchive> {
        T::encoder()
    }

    pub(crate) fn into_reference(self) -> T {
        self.value
    }

    pub(crate) fn archive_counts() -> (usize, usize) {
        let counters = T::counters();
        (
            counters.writes.load(AtomicOrdering::Relaxed),
            counters.reads.load(AtomicOrdering::Relaxed),
        )
    }
}

impl<T: FixedWeight> From<T> for NativeWeight<T> {
    fn from(value: T) -> Self {
        Self {
            value,
            encoder: T::encoder(),
        }
    }
}

impl From<NativeSum> for Sum {
    fn from(value: NativeSum) -> Self {
        value.into_reference()
    }
}

impl<T: FixedWeight + HasZero> Default for NativeWeight<T> {
    fn default() -> Self {
        T::zero().into()
    }
}
impl<T: Debug> Debug for NativeWeight<T> {
    fn fmt(&self, output: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(output)
    }
}
impl<T: PartialEq> PartialEq for NativeWeight<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Eq> Eq for NativeWeight<T> {}
impl<T: Ord> PartialOrd for NativeWeight<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord> Ord for NativeWeight<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}
impl<T: Hash> Hash for NativeWeight<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}
impl<T: SizeOf> SizeOf for NativeWeight<T> {
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.value.size_of_children(context);
    }
}
impl<T> dbsp::utils::SupportsRoaring for NativeWeight<T> {}
dbsp::never_none_1!(NativeWeight);

impl<T: FixedWeight + HasZero> HasZero for NativeWeight<T> {
    fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    fn zero() -> Self {
        Self::default()
    }
}
impl<T: AddByRef> AddByRef for NativeWeight<T> {
    fn add_by_ref(&self, other: &Self) -> Self {
        Self {
            value: self.value.add_by_ref(&other.value),
            encoder: self.encoder.clone(),
        }
    }
}
impl<T: AddAssignByRef> AddAssignByRef for NativeWeight<T> {
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.value.add_assign_by_ref(&other.value);
    }
}
impl<T: MulByRef<i64, Output = T>> MulByRef<i64> for NativeWeight<T> {
    type Output = Self;
    fn mul_by_ref(&self, weight: &i64) -> Self {
        Self {
            value: self.value.mul_by_ref(weight),
            encoder: self.encoder.clone(),
        }
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ArchivedWeight<A>(A);

impl<A: OrdRepr<T>, T> OrdRepr<NativeWeight<T>> for ArchivedWeight<A> {
    fn ord_cmp(&self, other: &NativeWeight<T>) -> Ordering {
        self.0.ord_cmp(&other.value)
    }
}
impl<A: HashRepr> HashRepr for ArchivedWeight<A> {
    const FAITHFUL: bool = A::FAITHFUL;
    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        self.0.hash_repr(state);
    }
}
impl<A, T: FixedWeight, D: Fallible + ?Sized> Deserialize<NativeWeight<T>, D> for ArchivedWeight<A>
where
    A: Deserialize<T, D>,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<NativeWeight<T>, D::Error> {
        let value = self.0.deserialize(deserializer)?;
        T::counters().reads.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(value.into())
    }
}
impl<T: FixedWeight> Archive for NativeWeight<T> {
    type Archived = ArchivedWeight<Archived<T>>;
    type Resolver = ArchivedBatch;

    unsafe fn resolve(&self, _: usize, batch: Self::Resolver, out: *mut Self::Archived) {
        let bytes = batch.rows().next().unwrap();
        assert_eq!(bytes.len(), size_of::<Self::Archived>());
        // SAFETY: rkyv supplies aligned writable space. FixedWeight supplies
        // exactly the initialized archived payload, and ArchivedWeight is
        // transparent over the original archived type.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out.cast(), bytes.len()) }
    }
}
impl<T: FixedWeight, S: Fallible + ?Sized> Serialize<S> for NativeWeight<T> {
    fn serialize(&self, _: &mut S) -> Result<Self::Resolver, S::Error> {
        let bytes = self.value.encode(&self.encoder);
        T::counters().writes.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(bytes)
    }
}
