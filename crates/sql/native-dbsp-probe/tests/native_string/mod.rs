//! Native rkyv writer for one nullable VARCHAR key layout.

use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{IsNone, SupportsRoaring, Tup1},
};
use feldera_native_probe::{CompiledStringArchive, StringArchiveLayout};
use feldera_sqllib::SqlString;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize, ser::Serializer};
use size_of::{Context, SizeOf};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    mem::{offset_of, size_of},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

pub type Row = Tup1<Option<SqlString>>;
type ArchivedRow = Archived<Row>;

pub static WRITES: AtomicUsize = AtomicUsize::new(0);
pub static READS: AtomicUsize = AtomicUsize::new(0);
pub static SEARCHES: AtomicUsize = AtomicUsize::new(0);

pub struct Encoder {
    compiled: CompiledStringArchive,
}

pub fn encoder() -> Arc<Encoder> {
    thread_local! {
        static LOCAL_ENCODER: RefCell<Weak<Encoder>> = const { RefCell::new(Weak::new()) };
    }
    LOCAL_ENCODER.with_borrow_mut(|cache| {
        if let Some(encoder) = cache.upgrade() {
            return encoder;
        }
        let encoder = shared_encoder();
        *cache = Arc::downgrade(&encoder);
        encoder
    })
}

fn shared_encoder() -> Arc<Encoder> {
    static CACHE: OnceLock<Mutex<Weak<Encoder>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Mutex::default).lock().unwrap();
    if let Some(encoder) = cache.upgrade() {
        return encoder;
    }
    let sample = rkyv::to_bytes::<_, 256>(&Tup1(Some(SqlString::from_ref("x")))).unwrap();
    // SAFETY: this is the aligned reference archive for exactly Row.
    let row = unsafe { rkyv::archived_root::<Row>(&sample) };
    let string = row.0.as_ref().unwrap();
    let header = std::ptr::from_ref(string) as usize - std::ptr::from_ref(row) as usize;
    let layout = StringArchiveLayout::new(
        size_of::<ArchivedRow>(),
        header,
        Some(offset_of!(ArchivedRow, 0)),
    )
    .unwrap();
    let encoder = Arc::new(Encoder {
        compiled: CompiledStringArchive::compile(layout).unwrap(),
    });
    *cache = Arc::downgrade(&encoder);
    encoder
}

#[derive(Clone)]
pub struct StringKey<const TRACE: bool> {
    pub value: Row,
}

pub type NativeString = StringKey<true>;
pub type MeasuredStringKey = StringKey<false>;

impl<const TRACE: bool> From<Row> for StringKey<TRACE> {
    fn from(value: Row) -> Self {
        Self { value }
    }
}

impl<const TRACE: bool> Default for StringKey<TRACE> {
    fn default() -> Self {
        Tup1(None).into()
    }
}

impl<const TRACE: bool> fmt::Debug for StringKey<TRACE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<const TRACE: bool> PartialEq for StringKey<TRACE> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<const TRACE: bool> Eq for StringKey<TRACE> {}

impl<const TRACE: bool> PartialOrd for StringKey<TRACE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const TRACE: bool> Ord for StringKey<TRACE> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<const TRACE: bool> Hash for StringKey<TRACE> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<const TRACE: bool> SizeOf for StringKey<TRACE> {
    fn size_of_children(&self, context: &mut Context) {
        self.value.size_of_children(context);
    }
}

impl<const TRACE: bool> SupportsRoaring for StringKey<TRACE> {}

impl<const TRACE: bool> IsNone for StringKey<TRACE> {
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

#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ArchivedStringKey<const TRACE: bool>(ArchivedRow);

impl<const TRACE: bool> OrdRepr<StringKey<TRACE>> for ArchivedStringKey<TRACE> {
    fn ord_cmp(&self, other: &StringKey<TRACE>) -> Ordering {
        if TRACE {
            SEARCHES.fetch_add(1, Relaxed);
        }
        self.0.ord_cmp(&other.value)
    }
}

impl<const TRACE: bool> HashRepr for ArchivedStringKey<TRACE> {
    const FAITHFUL: bool = true;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        self.0.hash_repr(state);
    }
}

impl<const TRACE: bool, D: Fallible + ?Sized> Deserialize<StringKey<TRACE>, D>
    for ArchivedStringKey<TRACE>
where
    ArchivedRow: Deserialize<Row, D>,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<StringKey<TRACE>, D::Error> {
        if TRACE {
            READS.fetch_add(1, Relaxed);
        }
        self.0.deserialize(deserializer).map(StringKey::from)
    }
}

pub enum Resolver {
    Native {
        encoder: Arc<Encoder>,
        payload_position: Option<usize>,
    },
    Reference(<Row as Archive>::Resolver),
}

impl<const TRACE: bool> Archive for StringKey<TRACE> {
    type Archived = ArchivedStringKey<TRACE>;
    type Resolver = Resolver;

    unsafe fn resolve(
        &self,
        position: usize,
        resolver: Resolver,
        out: *mut ArchivedStringKey<TRACE>,
    ) {
        match resolver {
            Resolver::Native {
                encoder,
                payload_position,
            } => {
                let mut root = [0; size_of::<ArchivedRow>()];
                encoder
                    .compiled
                    .encode_single_root_into(
                        self.value.0.as_ref().map(SqlString::str),
                        position,
                        payload_position,
                        &mut root,
                    )
                    .expect("serializer produced a valid string payload position");
                // SAFETY: the native root has exactly the checked ArchivedRow
                // layout. The transparent wrapper has the same layout, and
                // any long-string pointer now targets its serialized payload.
                unsafe {
                    std::ptr::copy_nonoverlapping(root.as_ptr(), out.cast::<u8>(), root.len());
                }
                if TRACE {
                    WRITES.fetch_add(1, Relaxed);
                }
            }
            Resolver::Reference(resolver) => {
                // SAFETY: the wrapper is transparent over ArchivedRow and this
                // resolver came from the reference row's own serializer.
                unsafe {
                    self.value
                        .resolve(position, resolver, out.cast::<ArchivedRow>())
                };
            }
        }
    }
}

impl<const TRACE: bool, S: Serializer + ?Sized> Serialize<S> for StringKey<TRACE>
where
    Row: Serialize<S>,
{
    fn serialize(&self, serializer: &mut S) -> Result<Resolver, S::Error> {
        let value = self.value.0.as_ref().map(SqlString::str);
        let encoder = encoder();
        if !encoder.compiled.supports_single_value(value) {
            return self.value.serialize(serializer).map(Resolver::Reference);
        }
        let payload_position = if let Some(value) = value.filter(|text| text.len() > 15) {
            let start = serializer.pos();
            serializer.write(value.as_bytes())?;
            Some(start)
        } else {
            None
        };
        Ok(Resolver::Native {
            encoder,
            payload_position,
        })
    }
}
