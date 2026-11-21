//! A prebuilt adapter for exactly one reference tuple layout. This uses the
//! existing DBSP factory graph but does not solve arbitrary runtime layouts.
use super::context::{Layout, NativeFactory, NativeRow};
use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{SupportsRoaring, Tup2},
};
use feldera_native_probe::{ArchiveField, ArchiveInteger};
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::{Context, SizeOf};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    mem::{align_of, offset_of, size_of},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

pub static NATIVE_WRITES: AtomicUsize = AtomicUsize::new(0);
pub static NATIVE_READS: AtomicUsize = AtomicUsize::new(0);
pub static ARCHIVED_SEARCHES: AtomicUsize = AtomicUsize::new(0);

pub fn layout() -> Arc<Layout> {
    static CACHE: OnceLock<Mutex<Weak<Layout>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Mutex::default).lock().unwrap();
    if let Some(layout) = cache.upgrade() {
        return layout;
    }
    type A = Archived<Tup2<i32, i64>>;
    let layout = Layout::compile(
        2,
        size_of::<A>(),
        align_of::<A>(),
        vec![
            ArchiveField {
                integer: ArchiveInteger::I32,
                payload_offset: offset_of!(A, 0),
                null_tag_offset: None,
            },
            ArchiveField {
                integer: ArchiveInteger::I64,
                payload_offset: offset_of!(A, 1),
                null_tag_offset: None,
            },
        ],
    );
    *cache = Arc::downgrade(&layout);
    layout
}

#[derive(Clone, Debug)]
pub struct NativePair(NativeRow);
impl NativePair {
    pub fn tuple(&self) -> Tup2<i32, i64> {
        Tup2(
            i32::try_from(self.0.values[0].unwrap()).unwrap(),
            i64::try_from(self.0.values[1].unwrap()).unwrap(),
        )
    }
}
impl From<Tup2<i32, i64>> for NativePair {
    fn from(value: Tup2<i32, i64>) -> Self {
        Self(NativeRow {
            layout: layout(),
            values: vec![Some(value.0.into()), Some(value.1.into())],
        })
    }
}
impl Default for NativePair {
    fn default() -> Self {
        Tup2(0, 0).into()
    }
}
impl PartialEq for NativePair {
    fn eq(&self, other: &Self) -> bool {
        self.tuple() == other.tuple()
    }
}
impl Eq for NativePair {}
impl PartialOrd for NativePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NativePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tuple().cmp(&other.tuple())
    }
}
impl Hash for NativePair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tuple().hash(state);
    }
}
impl SupportsRoaring for NativePair {}
impl SizeOf for NativePair {
    fn size_of_children(&self, context: &mut Context) {
        self.0.size_of_children(context);
    }
}
dbsp::never_none!(NativePair);

#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ArchivedPair(Archived<Tup2<i32, i64>>);
impl OrdRepr<NativePair> for ArchivedPair {
    fn ord_cmp(&self, other: &NativePair) -> Ordering {
        ARCHIVED_SEARCHES.fetch_add(1, AtomicOrdering::Relaxed);
        self.0.ord_cmp(&other.tuple())
    }
}
impl HashRepr for ArchivedPair {
    const FAITHFUL: bool = true;
    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        self.0.hash_repr(state);
    }
}
impl<D: Fallible + ?Sized> Deserialize<NativePair, D> for ArchivedPair {
    fn deserialize(&self, _deserializer: &mut D) -> Result<NativePair, D::Error> {
        NATIVE_READS.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(Tup2(self.0.0, self.0.1).into())
    }
}
impl Archive for NativePair {
    type Archived = ArchivedPair;
    type Resolver = Vec<u8>;
    unsafe fn resolve(&self, _pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        assert_eq!(resolver.len(), size_of::<Self::Archived>());
        // SAFETY: rkyv provides aligned writable space for ArchivedPair. The
        // private serializer produces exactly the validated native archive of
        // Tup2<i32,i64>; ArchivedPair is a transparent wrapper of that type.
        // Source/destination are disjoint and the checked length covers all bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(resolver.as_ptr(), out.cast::<u8>(), resolver.len());
        }
    }
}
impl<S: Fallible + ?Sized> Serialize<S> for NativePair {
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let factory = NativeFactory {
            layout: self.0.layout.clone(),
        };
        let bytes = factory.archive(&self.0).to_vec();
        NATIVE_WRITES.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(bytes)
    }
}
