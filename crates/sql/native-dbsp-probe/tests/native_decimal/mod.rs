//! One prebuilt nullable-decimal archive adapter for an actual inventory key.
use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{SupportsRoaring, Tup1},
};
use feldera_fxp::Fixed;
use feldera_native_probe::{ArchiveField, ArchiveInteger, ArchiveLayout, CompiledArchive};
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::{Context, SizeOf};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    mem::{offset_of, size_of},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};
pub type Decimal = Fixed<6, 2>;
pub type Row = Tup1<Option<Decimal>>;
pub static WRITES: AtomicUsize = AtomicUsize::new(0);
pub static READS: AtomicUsize = AtomicUsize::new(0);
pub static SEARCHES: AtomicUsize = AtomicUsize::new(0);

pub fn encoder() -> Arc<CompiledArchive> {
    thread_local! {
        static LOCAL_ENCODER: RefCell<Weak<CompiledArchive>> = const { RefCell::new(Weak::new()) };
    }
    LOCAL_ENCODER.with_borrow_mut(|local| {
        if let Some(encoder) = local.upgrade() {
            return encoder;
        }
        let encoder = shared_encoder();
        *local = Arc::downgrade(&encoder);
        encoder
    })
}

fn shared_encoder() -> Arc<CompiledArchive> {
    static CACHE: OnceLock<Mutex<Weak<CompiledArchive>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Mutex::default).lock().unwrap();
    if let Some(encoder) = cache.upgrade() {
        return encoder;
    }
    type A = Archived<Row>;
    let sample = rkyv::option::ArchivedOption::Some(Decimal::default());
    let rkyv::option::ArchivedOption::Some(value) = &sample else {
        unreachable!()
    };
    let payload = std::ptr::from_ref(value) as usize - std::ptr::from_ref(&sample) as usize;
    let encoder = Arc::new(
        CompiledArchive::compile(
            ArchiveLayout::new(
                size_of::<A>(),
                vec![ArchiveField {
                    integer: ArchiveInteger::I128,
                    payload_offset: offset_of!(A, 0) + payload,
                    null_tag_offset: Some(offset_of!(A, 0)),
                }],
            )
            .unwrap(),
        )
        .unwrap(),
    );
    *cache = Arc::downgrade(&encoder);
    encoder
}
#[derive(Clone)]
pub struct NativeDecimal {
    pub value: Row,
    encoder: Arc<CompiledArchive>,
}
impl fmt::Debug for NativeDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}
impl From<Row> for NativeDecimal {
    fn from(value: Row) -> Self {
        Self {
            value,
            encoder: encoder(),
        }
    }
}
impl Default for NativeDecimal {
    fn default() -> Self {
        Tup1(None).into()
    }
}
impl PartialEq for NativeDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl Eq for NativeDecimal {}
impl PartialOrd for NativeDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NativeDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}
impl Hash for NativeDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state)
    }
}
impl SizeOf for NativeDecimal {
    fn size_of_children(&self, _context: &mut Context) {}
}
impl SupportsRoaring for NativeDecimal {}
dbsp::never_none!(NativeDecimal);
#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ArchivedDecimal(Archived<Row>);
impl OrdRepr<NativeDecimal> for ArchivedDecimal {
    fn ord_cmp(&self, other: &NativeDecimal) -> Ordering {
        SEARCHES.fetch_add(1, AtomicOrdering::Relaxed);
        self.0.ord_cmp(&other.value)
    }
}
impl HashRepr for ArchivedDecimal {
    const FAITHFUL: bool = true;
    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        self.0.hash_repr(state)
    }
}
impl<D: Fallible + ?Sized> Deserialize<NativeDecimal, D> for ArchivedDecimal {
    fn deserialize(&self, _deserializer: &mut D) -> Result<NativeDecimal, D::Error> {
        READS.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(Tup1(self.0.0.as_ref().copied()).into())
    }
}
impl Archive for NativeDecimal {
    type Archived = ArchivedDecimal;
    type Resolver = [u8; size_of::<Archived<Row>>()];
    unsafe fn resolve(&self, _pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        // SAFETY: the native encoder wrote the verified Tup1<Option<Fixed<6,2>>>
        // layout. ArchivedDecimal is transparent over that representation;
        // rkyv supplies aligned writable space disjoint from the resolver.
        unsafe {
            std::ptr::copy_nonoverlapping(resolver.as_ptr(), out.cast::<u8>(), resolver.len());
        }
    }
}
impl<S: Fallible + ?Sized> Serialize<S> for NativeDecimal {
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let mut bytes = [0; size_of::<Archived<Row>>()];
        self.encoder
            .encode_wide_row_into(&[self.value.0.map(|value| value.mantissa())], &mut bytes)
            .unwrap();
        WRITES.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(bytes)
    }
}
