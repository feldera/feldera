//! FlatVariant: a flat-buffer implementation of the SQL VARIANT type.
//!
//! One contiguous byte buffer holds an entire VARIANT document; sub-values
//! are ranges into the shared buffer. Compared to [`crate::variant::Variant`]
//! (a recursive enum over `Arc<BTreeMap>`/`Arc<Vec>`), building a value from
//! JSON is a linear buffer write, rkyv serialization is one bulk write,
//! deserialization is one allocation plus a memcpy, drop is a single dealloc,
//! and equality is usually a memcmp.
//!
//! Programs opt in with `SET feldera_flat_variant = 'on'` (or globally with the
//! `FELDERA_FLAT_VARIANT` environment variable), which makes the SQL compiler
//! emit `FlatVariant` for VARIANT columns and the `FV` function-name grid
//! (`cast_to_FV_*`, `indexFV*`, `parse_json_fv`, `to_json_FV`, `typeof_fv`,
//! `variantnull_fv`; see `flat_variant::casts`). Connector metadata keeps the enum
//! `Variant`.
//!
//! # Encoding
//!
//! Every encoded value is `[tag: u8][body]` and is self-contained: a
//! sub-slice covering one value is itself a valid encoded value. Tags equal
//! the `Variant` enum discriminants, so the type-rank part of Ord is a byte
//! compare and the two types sort identically.
//!
//! Scalar bodies are fixed-width little-endian; String/Binary bodies are raw
//! bytes (length implied by the value extent). Containers:
//!
//! ```text
//! Array:  [count: u32][end_i: u32 x count][child bytes x count]
//! Map:    [count: u32][key_end_i: u32 x count][val_end_i: u32 x count]
//!         [key bytes x count][val bytes x count]
//! ```
//!
//! `end_i` is the cumulative END of element i relative to the start of its
//! payload area: element i occupies `[end_{i-1}, end_i)` with `end_{-1} = 0`,
//! giving O(1) element access and O(log n) map lookup. Map keys are complete
//! encoded values (any Variant kind can be a key) stored sorted ascending,
//! which makes the encoding canonical: byte equality implies value equality.
//! The reverse holds except through float payloads (negative zero and NaN
//! compare equal across different bit patterns), so `Eq` uses a memcmp fast
//! path with a structural fallback and `Hash` delegates float payloads to
//! the `F32`/`F64` wrappers.

#[doc(hidden)]
pub mod casts;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use dbsp::algebra::{F32, F64};
use feldera_fxp::DynamicDecimal;
use feldera_macros::IsNone;
use feldera_types::serde_with_context::serde_config::VariantFormat;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use rkyv::ser::{ScratchSpace, Serializer as RkyvSerializer};
use rkyv::vec::{ArchivedVec, VecResolver};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use size_of::SizeOf;

use crate::variant::Variant;
use crate::{
    ByteArray, Date, LongInterval, ShortInterval, SqlString, Time, Timestamp, TimestampTz, Uuid,
};

// Tags equal the Variant enum discriminants (variant.rs declaration order).
const TAG_SQL_NULL: u8 = 0;
const TAG_VARIANT_NULL: u8 = 1;
const TAG_BOOLEAN: u8 = 2;
const TAG_TINYINT: u8 = 3;
const TAG_SMALLINT: u8 = 4;
const TAG_INT: u8 = 5;
const TAG_BIGINT: u8 = 6;
const TAG_UTINYINT: u8 = 7;
const TAG_USMALLINT: u8 = 8;
const TAG_UINT: u8 = 9;
const TAG_UBIGINT: u8 = 10;
const TAG_REAL: u8 = 11;
const TAG_DOUBLE: u8 = 12;
const TAG_DECIMAL: u8 = 13;
const TAG_STRING: u8 = 14;
const TAG_DATE: u8 = 15;
const TAG_TIME: u8 = 16;
const TAG_TIMESTAMP: u8 = 17;
const TAG_SHORT_INTERVAL: u8 = 18;
const TAG_LONG_INTERVAL: u8 = 19;
const TAG_BINARY: u8 = 20;
const TAG_GEOMETRY: u8 = 21;
const TAG_UUID: u8 = 22;
const TAG_ARRAY: u8 = 23;
const TAG_MAP: u8 = 24;
const TAG_TIMESTAMP_TZ: u8 = 25;

// The type

/// A SQL VARIANT value stored as one flat, canonically encoded byte buffer.
///
/// Cloning bumps a refcount; sub-value access (`index`, `index_string`)
/// returns views into the shared buffer without copying the subtree.
#[derive(Clone, IsNone)]
pub struct FlatVariant {
    buf: Arc<[u8]>,
    start: u32,
    len: u32,
}

impl FlatVariant {
    /// Wrap a complete encoded document.
    ///
    /// The bytes must be a valid encoding (produced by this module); no
    /// validation is performed beyond non-emptiness.
    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(!bytes.is_empty(), "encoded value cannot be empty");
        FlatVariant {
            buf: Arc::from(bytes),
            start: 0,
            len: bytes.len() as u32,
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start as usize..(self.start + self.len) as usize]
    }

    /// A sub-value sharing this value's buffer; `range` is relative to
    /// `self.as_bytes()`.
    fn subvalue(&self, range: Range<usize>) -> FlatVariant {
        FlatVariant {
            buf: self.buf.clone(),
            start: self.start + range.start as u32,
            len: (range.end - range.start) as u32,
        }
    }

    /// The SQL NULL value (`Variant::SqlNull` equivalent).
    pub fn sql_null() -> FlatVariant {
        static NULL: OnceLock<Arc<[u8]>> = OnceLock::new();
        let buf = NULL.get_or_init(|| Arc::from(&[TAG_SQL_NULL][..])).clone();
        FlatVariant {
            buf,
            start: 0,
            len: 1,
        }
    }

    /// The JSON null value (`Variant::VariantNull` equivalent).
    pub fn variant_null() -> FlatVariant {
        static NULL: OnceLock<Arc<[u8]>> = OnceLock::new();
        let buf = NULL
            .get_or_init(|| Arc::from(&[TAG_VARIANT_NULL][..]))
            .clone();
        FlatVariant {
            buf,
            start: 0,
            len: 1,
        }
    }

    /// Map lookup by string key; missing key or non-map yields SqlNull.
    /// Same semantics as `Variant::index_string`, but the result shares the
    /// buffer instead of cloning the subtree.
    pub fn index_string<I: AsRef<str>>(&self, index: I) -> FlatVariant {
        let bytes = self.as_bytes();
        if bytes[0] != TAG_MAP {
            return FlatVariant::sql_null();
        }
        let key = index.as_ref();
        let mut probe = Vec::with_capacity(1 + key.len());
        probe.push(TAG_STRING);
        probe.extend_from_slice(key.as_bytes());
        match self.find_key(&probe) {
            Some(i) => self.subvalue(Container::new(bytes).map_value(i)),
            None => FlatVariant::sql_null(),
        }
    }

    /// Same semantics as `Variant::index`: 1-based array indexing with
    /// numeric key coercion, map lookup by exact key, None otherwise.
    pub fn index(&self, index: &FlatVariant) -> Option<FlatVariant> {
        let bytes = self.as_bytes();
        match bytes[0] {
            TAG_ARRAY => {
                let i = index.as_isize()?;
                let c = Container::new(bytes);
                // SQL uses 1-based indexing.
                let i = usize::try_from(i - 1).ok()?;
                (i < c.count).then(|| self.subvalue(c.element(i)))
            }
            TAG_MAP => self
                .find_key(index.as_bytes())
                .map(|i| self.subvalue(Container::new(bytes).map_value(i))),
            _ => None,
        }
    }

    /// Binary search the sorted key area of a map for an encoded key.
    fn find_key(&self, probe: &[u8]) -> Option<usize> {
        let bytes = self.as_bytes();
        let c = Container::new(bytes);
        let mut lo = 0usize;
        let mut hi = c.count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            match cmp_values(&bytes[c.element(mid)], probe) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Integer value of a numeric scalar, for 1-based array indexing.
    fn as_isize(&self) -> Option<isize> {
        let b = self.as_bytes();
        let p = &b[1..];
        Some(match b[0] {
            TAG_TINYINT => (p[0] as i8) as isize,
            TAG_SMALLINT => i16::from_le_bytes(p.try_into().unwrap()) as isize,
            TAG_INT => i32::from_le_bytes(p.try_into().unwrap()) as isize,
            TAG_BIGINT => i64::from_le_bytes(p.try_into().unwrap()) as isize,
            TAG_UTINYINT => p[0] as isize,
            TAG_USMALLINT => u16::from_le_bytes(p.try_into().unwrap()) as isize,
            TAG_UINT => u32::from_le_bytes(p.try_into().unwrap()) as isize,
            TAG_UBIGINT => isize::try_from(u64::from_le_bytes(p.try_into().unwrap())).ok()?,
            _ => return None,
        })
    }

    pub fn to_json_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(serde_json::to_string(self)?)
    }
}

impl Default for FlatVariant {
    fn default() -> Self {
        FlatVariant::sql_null()
    }
}

impl PartialEq for FlatVariant {
    fn eq(&self, other: &Self) -> bool {
        eq_values(self.as_bytes(), other.as_bytes())
    }
}

impl Eq for FlatVariant {}

impl Ord for FlatVariant {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_values(self.as_bytes(), other.as_bytes())
    }
}

impl PartialOrd for FlatVariant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for FlatVariant {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(self.as_bytes(), state);
    }
}

impl fmt::Debug for FlatVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlatVariant({:?})", Variant::from(self))
    }
}

impl SizeOf for FlatVariant {
    fn size_of_children(&self, context: &mut size_of::Context) {
        // The whole document buffer, shared by all sub-values holding it.
        context.add(self.buf.len());
    }
}

// Container access

struct Container<'a> {
    body: &'a [u8],
    count: usize,
    is_map: bool,
}

#[inline]
fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

impl<'a> Container<'a> {
    /// `value` is one complete encoded value with an Array or Map tag.
    fn new(value: &'a [u8]) -> Self {
        let is_map = match value[0] {
            TAG_ARRAY => false,
            TAG_MAP => true,
            tag => panic!("not a container tag: {tag}"),
        };
        let body = &value[1..];
        let count = read_u32(body, 0) as usize;
        Container {
            body,
            count,
            is_map,
        }
    }

    /// Offset-table entry `i` of table 0 (elements or keys) or 1 (map values).
    #[inline]
    fn end(&self, table: usize, i: usize) -> usize {
        read_u32(self.body, 4 + (table * self.count + i) * 4) as usize
    }

    #[inline]
    fn payload_base(&self) -> usize {
        4 + (if self.is_map { 2 } else { 1 }) * self.count * 4
    }

    /// Range of element `i` (array) or key `i` (map), relative to the start
    /// of the complete value (including its tag byte).
    fn element(&self, i: usize) -> Range<usize> {
        debug_assert!(i < self.count);
        let base = self.payload_base();
        let start = if i == 0 { 0 } else { self.end(0, i - 1) };
        // +1 converts from body-relative to value-relative.
        1 + base + start..1 + base + self.end(0, i)
    }

    /// Range of map value `i`, relative to the start of the complete value.
    fn map_value(&self, i: usize) -> Range<usize> {
        debug_assert!(self.is_map && i < self.count);
        let keys_total = if self.count == 0 {
            0
        } else {
            self.end(0, self.count - 1)
        };
        let base = self.payload_base() + keys_total;
        let start = if i == 0 { 0 } else { self.end(1, i - 1) };
        1 + base + start..1 + base + self.end(1, i)
    }
}

// Comparison and hashing over encoded bytes

#[inline]
fn f32_at(b: &[u8], at: usize) -> F32 {
    F32::new(f32::from_le_bytes(b[at..at + 4].try_into().unwrap()))
}

#[inline]
fn f64_at(b: &[u8], at: usize) -> F64 {
    F64::new(f64::from_le_bytes(b[at..at + 8].try_into().unwrap()))
}

/// Total order over two complete encoded values; matches the derived Ord of
/// the `Variant` enum exactly, including the lexicographic (i128, u8) order
/// of SqlDecimal and the F32/F64 total order.
fn cmp_values(a: &[u8], b: &[u8]) -> Ordering {
    let (ta, tb) = (a[0], b[0]);
    if ta != tb {
        return ta.cmp(&tb);
    }
    let (pa, pb) = (&a[1..], &b[1..]);
    match ta {
        TAG_SQL_NULL | TAG_VARIANT_NULL => Ordering::Equal,
        TAG_TINYINT => (pa[0] as i8).cmp(&(pb[0] as i8)),
        TAG_SMALLINT => i16::from_le_bytes(pa.try_into().unwrap())
            .cmp(&i16::from_le_bytes(pb.try_into().unwrap())),
        TAG_INT | TAG_DATE | TAG_LONG_INTERVAL => i32::from_le_bytes(pa.try_into().unwrap())
            .cmp(&i32::from_le_bytes(pb.try_into().unwrap())),
        TAG_BIGINT | TAG_TIMESTAMP | TAG_SHORT_INTERVAL | TAG_TIMESTAMP_TZ => {
            i64::from_le_bytes(pa.try_into().unwrap())
                .cmp(&i64::from_le_bytes(pb.try_into().unwrap()))
        }
        TAG_BOOLEAN | TAG_UTINYINT => pa[0].cmp(&pb[0]),
        TAG_USMALLINT => u16::from_le_bytes(pa.try_into().unwrap())
            .cmp(&u16::from_le_bytes(pb.try_into().unwrap())),
        TAG_UINT => u32::from_le_bytes(pa.try_into().unwrap())
            .cmp(&u32::from_le_bytes(pb.try_into().unwrap())),
        TAG_UBIGINT | TAG_TIME => u64::from_le_bytes(pa.try_into().unwrap())
            .cmp(&u64::from_le_bytes(pb.try_into().unwrap())),
        TAG_REAL => f32_at(pa, 0).cmp(&f32_at(pb, 0)),
        TAG_DOUBLE => f64_at(pa, 0).cmp(&f64_at(pb, 0)),
        TAG_GEOMETRY => (f64_at(pa, 0), f64_at(pa, 8)).cmp(&(f64_at(pb, 0), f64_at(pb, 8))),
        TAG_DECIMAL => {
            let da = i128::from_le_bytes(pa[..16].try_into().unwrap());
            let db = i128::from_le_bytes(pb[..16].try_into().unwrap());
            (da, pa[16]).cmp(&(db, pb[16]))
        }
        // str Ord and SmallVec<u8> Ord are both bytewise; Uuid stores
        // big-endian bytes whose slice order equals uuid::Uuid Ord.
        TAG_STRING | TAG_BINARY | TAG_UUID => pa.cmp(pb),
        TAG_ARRAY => {
            let (ca, cb) = (Container::new(a), Container::new(b));
            for i in 0..ca.count.min(cb.count) {
                let ord = cmp_values(&a[ca.element(i)], &b[cb.element(i)]);
                if ord.is_ne() {
                    return ord;
                }
            }
            ca.count.cmp(&cb.count)
        }
        TAG_MAP => {
            // BTreeMap Ord: lexicographic over (key, value) pairs in
            // ascending key order, then length. Keys are stored sorted.
            let (ca, cb) = (Container::new(a), Container::new(b));
            for i in 0..ca.count.min(cb.count) {
                let ord = cmp_values(&a[ca.element(i)], &b[cb.element(i)]);
                if ord.is_ne() {
                    return ord;
                }
                let ord = cmp_values(&a[ca.map_value(i)], &b[cb.map_value(i)]);
                if ord.is_ne() {
                    return ord;
                }
            }
            ca.count.cmp(&cb.count)
        }
        tag => unreachable!("invalid tag {tag}"),
    }
}

/// Equality with a byte-compare fast path; falls back to the structural walk
/// because float payloads can compare equal across different bit patterns.
fn eq_values(a: &[u8], b: &[u8]) -> bool {
    a == b || cmp_values(a, b).is_eq()
}

/// Hash one encoded value, consistent with `cmp_values` equality: float
/// payloads delegate to the F32/F64 Hash impls (which normalize exactly as
/// their Eq does); every other payload is equal iff its bytes are.
fn hash_value<H: Hasher>(value: &[u8], state: &mut H) {
    let tag = value[0];
    state.write_u8(tag);
    let payload = &value[1..];
    match tag {
        TAG_REAL => f32_at(payload, 0).hash(state),
        TAG_DOUBLE => f64_at(payload, 0).hash(state),
        TAG_GEOMETRY => {
            f64_at(payload, 0).hash(state);
            f64_at(payload, 8).hash(state);
        }
        TAG_ARRAY => {
            let c = Container::new(value);
            state.write_usize(c.count);
            for i in 0..c.count {
                hash_value(&value[c.element(i)], state);
            }
        }
        TAG_MAP => {
            let c = Container::new(value);
            state.write_usize(c.count);
            for i in 0..c.count {
                hash_value(&value[c.element(i)], state);
                hash_value(&value[c.map_value(i)], state);
            }
        }
        _ => state.write(payload),
    }
}

// Writer

/// Appends encoded values to a scratch buffer. Children are encoded first,
/// each as a complete value somewhere in the scratch; containers are then
/// assembled after their children with `extend_from_within`, so bytes are
/// copied O(depth) times and nothing is allocated per node.
struct Writer {
    out: Vec<u8>,
}

impl Writer {
    #[inline]
    fn scalar(&mut self, tag: u8, payload: &[u8]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(tag);
        self.out.extend_from_slice(payload);
        start..self.out.len()
    }

    /// Assemble an array from child value ranges within this writer.
    fn array(&mut self, children: &[Range<usize>]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(TAG_ARRAY);
        let count = children.len() as u32;
        self.out.extend_from_slice(&count.to_le_bytes());
        let mut end = 0u32;
        for r in children {
            end += r.len() as u32;
            self.out.extend_from_slice(&end.to_le_bytes());
        }
        for r in children {
            self.out.extend_from_within(r.clone());
        }
        start..self.out.len()
    }

    /// Assemble a map from (key, value) ranges within this writer. Entries
    /// must be sorted ascending by encoded key with no duplicates.
    fn map(&mut self, entries: &[(Range<usize>, Range<usize>)]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(TAG_MAP);
        let count = entries.len() as u32;
        self.out.extend_from_slice(&count.to_le_bytes());
        let mut end = 0u32;
        for (k, _) in entries {
            end += k.len() as u32;
            self.out.extend_from_slice(&end.to_le_bytes());
        }
        let mut end = 0u32;
        for (_, v) in entries {
            end += v.len() as u32;
            self.out.extend_from_slice(&end.to_le_bytes());
        }
        for (k, _) in entries {
            self.out.extend_from_within(k.clone());
        }
        for (_, v) in entries {
            self.out.extend_from_within(v.clone());
        }
        start..self.out.len()
    }
}

/// Sort map entries by encoded key and drop duplicate keys, keeping the last
/// occurrence (BTreeMap insert semantics: a later insert overwrites).
fn sort_map_entries(out: &[u8], entries: &mut Vec<(Range<usize>, Range<usize>)>) {
    entries.sort_by(|a, b| cmp_values(&out[a.0.clone()], &out[b.0.clone()]));
    let mut w = 0;
    for i in 0..entries.len() {
        let last_of_run = i + 1 == entries.len()
            || cmp_values(&out[entries[i].0.clone()], &out[entries[i + 1].0.clone()]).is_ne();
        if last_of_run {
            entries.swap(w, i);
            w += 1;
        }
    }
    entries.truncate(w);
}

// Conversion from/to the enum Variant

/// Encode one `Variant` into the writer; returns the value's range.
///
/// Panics on `Variant::Geometry`: `GeoPoint` exposes no accessors, JSON
/// input can never produce it, and no cast from GEOMETRY to VARIANT exists
/// for FlatVariant programs.
fn encode_variant(w: &mut Writer, v: &Variant) -> Range<usize> {
    match v {
        Variant::SqlNull => w.scalar(TAG_SQL_NULL, &[]),
        Variant::VariantNull => w.scalar(TAG_VARIANT_NULL, &[]),
        Variant::Boolean(b) => w.scalar(TAG_BOOLEAN, &[*b as u8]),
        Variant::TinyInt(x) => w.scalar(TAG_TINYINT, &x.to_le_bytes()),
        Variant::SmallInt(x) => w.scalar(TAG_SMALLINT, &x.to_le_bytes()),
        Variant::Int(x) => w.scalar(TAG_INT, &x.to_le_bytes()),
        Variant::BigInt(x) => w.scalar(TAG_BIGINT, &x.to_le_bytes()),
        Variant::UTinyInt(x) => w.scalar(TAG_UTINYINT, &x.to_le_bytes()),
        Variant::USmallInt(x) => w.scalar(TAG_USMALLINT, &x.to_le_bytes()),
        Variant::UInt(x) => w.scalar(TAG_UINT, &x.to_le_bytes()),
        Variant::UBigInt(x) => w.scalar(TAG_UBIGINT, &x.to_le_bytes()),
        Variant::Real(x) => w.scalar(TAG_REAL, &x.into_inner().to_le_bytes()),
        Variant::Double(x) => w.scalar(TAG_DOUBLE, &x.into_inner().to_le_bytes()),
        Variant::SqlDecimal((sig, scale)) => {
            let mut payload = [0u8; 17];
            payload[..16].copy_from_slice(&sig.to_le_bytes());
            payload[16] = *scale;
            w.scalar(TAG_DECIMAL, &payload)
        }
        Variant::String(s) => w.scalar(TAG_STRING, s.str().as_bytes()),
        Variant::Date(d) => w.scalar(TAG_DATE, &d.days().to_le_bytes()),
        Variant::Time(t) => w.scalar(TAG_TIME, &t.nanoseconds().to_le_bytes()),
        Variant::Timestamp(t) => w.scalar(TAG_TIMESTAMP, &t.microseconds().to_le_bytes()),
        Variant::TimestampTz(t) => w.scalar(TAG_TIMESTAMP_TZ, &t.microseconds().to_le_bytes()),
        Variant::ShortInterval(i) => w.scalar(TAG_SHORT_INTERVAL, &i.microseconds().to_le_bytes()),
        Variant::LongInterval(i) => w.scalar(TAG_LONG_INTERVAL, &i.months().to_le_bytes()),
        Variant::Binary(b) => w.scalar(TAG_BINARY, b.as_slice()),
        Variant::Geometry(g) => {
            let mut payload = [0u8; 16];
            payload[..8].copy_from_slice(&g.left().into_inner().to_le_bytes());
            payload[8..].copy_from_slice(&g.right().into_inner().to_le_bytes());
            w.scalar(TAG_GEOMETRY, &payload)
        }
        Variant::Uuid(u) => w.scalar(TAG_UUID, &u.to_bytes()[..]),
        Variant::Array(items) => {
            let children: Vec<Range<usize>> =
                items.iter().map(|item| encode_variant(w, item)).collect();
            w.array(&children)
        }
        Variant::Map(map) => {
            // BTreeMap iterates in Variant Ord order, which equals
            // cmp_values order on the encodings: sorted and deduplicated.
            let entries: Vec<(Range<usize>, Range<usize>)> = map
                .iter()
                .map(|(k, val)| (encode_variant(w, k), encode_variant(w, val)))
                .collect();
            w.map(&entries)
        }
    }
}

impl From<&Variant> for FlatVariant {
    fn from(v: &Variant) -> Self {
        let mut w = Writer {
            out: Vec::with_capacity(256),
        };
        let range = encode_variant(&mut w, v);
        FlatVariant::from_bytes(&w.out[range])
    }
}

/// Decode one complete encoded value back into a `Variant`.
fn decode_variant(bytes: &[u8]) -> Variant {
    let payload = &bytes[1..];
    match bytes[0] {
        TAG_SQL_NULL => Variant::SqlNull,
        TAG_VARIANT_NULL => Variant::VariantNull,
        TAG_BOOLEAN => Variant::Boolean(payload[0] != 0),
        TAG_TINYINT => Variant::TinyInt(payload[0] as i8),
        TAG_SMALLINT => Variant::SmallInt(i16::from_le_bytes(payload.try_into().unwrap())),
        TAG_INT => Variant::Int(i32::from_le_bytes(payload.try_into().unwrap())),
        TAG_BIGINT => Variant::BigInt(i64::from_le_bytes(payload.try_into().unwrap())),
        TAG_UTINYINT => Variant::UTinyInt(payload[0]),
        TAG_USMALLINT => Variant::USmallInt(u16::from_le_bytes(payload.try_into().unwrap())),
        TAG_UINT => Variant::UInt(u32::from_le_bytes(payload.try_into().unwrap())),
        TAG_UBIGINT => Variant::UBigInt(u64::from_le_bytes(payload.try_into().unwrap())),
        TAG_REAL => Variant::Real(f32::from_le_bytes(payload.try_into().unwrap()).into()),
        TAG_DOUBLE => Variant::Double(f64::from_le_bytes(payload.try_into().unwrap()).into()),
        TAG_DECIMAL => Variant::SqlDecimal((
            i128::from_le_bytes(payload[..16].try_into().unwrap()),
            payload[16],
        )),
        TAG_STRING => Variant::String(SqlString::from_ref(
            std::str::from_utf8(payload).expect("encoded string must be UTF-8"),
        )),
        TAG_DATE => Variant::Date(Date::from_days(i32::from_le_bytes(
            payload.try_into().unwrap(),
        ))),
        TAG_TIME => Variant::Time(Time::from_nanoseconds(u64::from_le_bytes(
            payload.try_into().unwrap(),
        ))),
        TAG_TIMESTAMP => Variant::Timestamp(Timestamp::from_microseconds(i64::from_le_bytes(
            payload.try_into().unwrap(),
        ))),
        TAG_TIMESTAMP_TZ => Variant::TimestampTz(TimestampTz::from_microseconds(
            i64::from_le_bytes(payload.try_into().unwrap()),
        )),
        TAG_SHORT_INTERVAL => Variant::ShortInterval(ShortInterval::from_microseconds(
            i64::from_le_bytes(payload.try_into().unwrap()),
        )),
        TAG_LONG_INTERVAL => Variant::LongInterval(LongInterval::from_months(i32::from_le_bytes(
            payload.try_into().unwrap(),
        ))),
        TAG_BINARY => Variant::Binary(ByteArray::new(payload)),
        TAG_GEOMETRY => Variant::Geometry(crate::GeoPoint::new(
            f64::from_le_bytes(payload[..8].try_into().unwrap()),
            f64::from_le_bytes(payload[8..].try_into().unwrap()),
        )),
        TAG_UUID => Variant::Uuid(Uuid::from_bytes(payload.try_into().unwrap())),
        TAG_ARRAY => {
            let c = Container::new(bytes);
            let items: Vec<Variant> = (0..c.count)
                .map(|i| decode_variant(&bytes[c.element(i)]))
                .collect();
            Variant::Array(items.into())
        }
        TAG_MAP => {
            let c = Container::new(bytes);
            let map: BTreeMap<Variant, Variant> = (0..c.count)
                .map(|i| {
                    (
                        decode_variant(&bytes[c.element(i)]),
                        decode_variant(&bytes[c.map_value(i)]),
                    )
                })
                .collect();
            Variant::Map(map.into())
        }
        tag => unreachable!("invalid tag {tag}"),
    }
}

impl From<&FlatVariant> for Variant {
    fn from(v: &FlatVariant) -> Self {
        decode_variant(v.as_bytes())
    }
}

impl From<FlatVariant> for Variant {
    fn from(v: FlatVariant) -> Self {
        Variant::from(&v)
    }
}

impl From<Option<FlatVariant>> for Variant {
    fn from(v: Option<FlatVariant>) -> Self {
        match v {
            None => Variant::SqlNull,
            Some(v) => Variant::from(&v),
        }
    }
}

/// Conversion is total: every enum value encodes. The fallible signature
/// matches the element bound of the enum's generic Array/Map conversions,
/// which lets `Array<FlatVariant>: TryFrom<Variant>` come from those impls.
impl TryFrom<Variant> for FlatVariant {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        Ok(FlatVariant::from(&value))
    }
}

// Mirrors the enum's behavior for VARIANT elements: `Option<Variant>` gets
// its `TryFrom<Variant>` from core's `From<T> for Option<T>`, which always
// wraps in `Some`. A JSON null inside a map or array therefore stays a
// variant-null VALUE (MapTests#mapValuesVariant depends on this); it does
// not become a Rust `None`.
impl TryFrom<Variant> for Option<FlatVariant> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Variant) -> Result<Self, Self::Error> {
        Ok(Some(FlatVariant::from(&value)))
    }
}

/// Constructors used by generated code for VARIANT literals and casts.
///
/// An explicit list instead of a blanket over `Variant: From<T>`: the
/// blanket would collide with the reflexive `From<FlatVariant>` once
/// `Variant: From<FlatVariant>` exists.
macro_rules! fv_from {
    ($($t:ty),* $(,)?) => {$(
        impl From<$t> for FlatVariant {
            fn from(value: $t) -> Self {
                FlatVariant::from(&Variant::from(value))
            }
        }
        impl From<Option<$t>> for FlatVariant {
            fn from(value: Option<$t>) -> Self {
                FlatVariant::from(&Variant::from(value))
            }
        }
    )*};
}

fv_from!(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    F32,
    F64,
    SqlString,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    ShortInterval,
    LongInterval,
    crate::GeoPoint,
    ByteArray,
    Uuid,
);

impl<const P: usize, const S: usize> From<crate::SqlDecimal<P, S>> for FlatVariant {
    fn from(value: crate::SqlDecimal<P, S>) -> Self {
        FlatVariant::from(&Variant::from(value))
    }
}

impl<const P: usize, const S: usize> From<Option<crate::SqlDecimal<P, S>>> for FlatVariant {
    fn from(value: Option<crate::SqlDecimal<P, S>>) -> Self {
        FlatVariant::from(&Variant::from(value))
    }
}

impl<T> From<crate::Array<T>> for FlatVariant
where
    Variant: From<T>,
    T: Clone,
{
    fn from(value: crate::Array<T>) -> Self {
        FlatVariant::from(&<Variant as From<crate::Array<T>>>::from(value))
    }
}

impl<T> From<Option<crate::Array<T>>> for FlatVariant
where
    Variant: From<T>,
    T: Clone,
{
    fn from(value: Option<crate::Array<T>>) -> Self {
        FlatVariant::from(&<Variant as From<Option<crate::Array<T>>>>::from(value))
    }
}

impl<K, V> From<crate::Map<K, V>> for FlatVariant
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    fn from(value: crate::Map<K, V>) -> Self {
        FlatVariant::from(&<Variant as From<crate::Map<K, V>>>::from(value))
    }
}

impl<K, V> From<Option<crate::Map<K, V>>> for FlatVariant
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    fn from(value: Option<crate::Map<K, V>>) -> Self {
        FlatVariant::from(&<Variant as From<Option<crate::Map<K, V>>>>::from(value))
    }
}

// rkyv: the archived form is the encoding itself

/// Archived form of [`FlatVariant`]: the encoded bytes, verbatim.
pub struct ArchivedFlatVariant {
    bytes: ArchivedVec<u8>,
}

impl ArchivedFlatVariant {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl rkyv::Archive for FlatVariant {
    type Archived = ArchivedFlatVariant;
    type Resolver = VecResolver;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = rkyv::out_field!(out.bytes);
        // SAFETY: `fo` points into `out`, which the caller guarantees is
        // valid for writes at the archived position, and the resolver was
        // produced by serializing exactly `self.as_bytes()`.
        unsafe { ArchivedVec::resolve_from_slice(self.as_bytes(), pos + fp, resolver, fo) };
    }
}

impl<S: ScratchSpace + RkyvSerializer + ?Sized> rkyv::Serialize<S> for FlatVariant {
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        ArchivedVec::serialize_from_slice(self.as_bytes(), serializer)
    }
}

impl<D: rkyv::Fallible + ?Sized> rkyv::Deserialize<FlatVariant, D> for ArchivedFlatVariant {
    fn deserialize(&self, _deserializer: &mut D) -> Result<FlatVariant, D::Error> {
        Ok(FlatVariant::from_bytes(self.as_bytes()))
    }
}

impl PartialEq for ArchivedFlatVariant {
    fn eq(&self, other: &Self) -> bool {
        eq_values(self.as_bytes(), other.as_bytes())
    }
}

impl Eq for ArchivedFlatVariant {}

impl Ord for ArchivedFlatVariant {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_values(self.as_bytes(), other.as_bytes())
    }
}

impl PartialOrd for ArchivedFlatVariant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for ArchivedFlatVariant {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(self.as_bytes(), state);
    }
}

// serde: JSON -> FlatVariant without an intermediate tree

/// Appends one complete encoded value to the writer, yielding its range.
/// Mirrors `VariantVisitor` (variant.rs) including the serde_json
/// arbitrary-precision number handling.
struct BuildValue<'w> {
    w: &'w mut Writer,
}

impl<'de> DeserializeSeed<'de> for BuildValue<'_> {
    type Value = Range<usize>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for BuildValue<'_> {
    type Value = Range<usize>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    #[inline]
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_BOOLEAN, &[value as u8]))
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_BIGINT, &value.to_le_bytes()))
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_UBIGINT, &value.to_le_bytes()))
    }

    #[inline]
    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_DECIMAL, &decimal_payload(value, 0)))
    }

    #[inline]
    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_DOUBLE, &value.to_le_bytes()))
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_STRING, value.as_bytes()))
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_STRING, value.as_bytes()))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_VARIANT_NULL, &[]))
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(self.w.scalar(TAG_VARIANT_NULL, &[]))
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut children = Vec::new();
        while let Some(range) = seq.next_element_seed(BuildValue { w: self.w })? {
            children.push(range);
        }
        Ok(self.w.array(&children))
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        match map.next_key_seed(KeyClassifier)? {
            Some(KeyClass::Number) => {
                let number: DynamicDecimal = map.next_value()?;
                Ok(self.w.scalar(
                    TAG_DECIMAL,
                    &decimal_payload(number.significand(), number.exponent()),
                ))
            }
            Some(KeyClass::Map(first_key)) => {
                let mut entries = Vec::new();
                let key_range = self.w.scalar(TAG_STRING, first_key.as_bytes());
                let val_range = map.next_value_seed(BuildValue { w: self.w })?;
                entries.push((key_range, val_range));
                while let Some(key) = map.next_key::<String>()? {
                    let key_range = self.w.scalar(TAG_STRING, key.as_bytes());
                    let val_range = map.next_value_seed(BuildValue { w: self.w })?;
                    entries.push((key_range, val_range));
                }
                sort_map_entries(&self.w.out, &mut entries);
                Ok(self.w.map(&entries))
            }
            None => Ok(self.w.map(&[])),
        }
    }
}

fn decimal_payload(significand: i128, scale: u8) -> [u8; 17] {
    let mut payload = [0u8; 17];
    payload[..16].copy_from_slice(&significand.to_le_bytes());
    payload[16] = scale;
    payload
}

/// serde_json's private token for arbitrary-precision numbers.
const DECIMAL_KEY_TOKEN: &str = "$serde_json::private::Number";

struct KeyClassifier;

enum KeyClass {
    Map(String),
    Number,
}

impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for KeyClassifier {
    type Value = KeyClass;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
        match s {
            DECIMAL_KEY_TOKEN => Ok(KeyClass::Number),
            _ => Ok(KeyClass::Map(s.to_owned())),
        }
    }

    fn visit_string<E>(self, s: String) -> Result<Self::Value, E> {
        match s.as_str() {
            DECIMAL_KEY_TOKEN => Ok(KeyClass::Number),
            _ => Ok(KeyClass::Map(s)),
        }
    }
}

impl<'de> Deserialize<'de> for FlatVariant {
    fn deserialize<D>(deserializer: D) -> Result<FlatVariant, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Reuse one scratch buffer per thread; the final document is copied
        // into a right-sized Arc.
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> =
                std::cell::RefCell::new(Vec::with_capacity(4096));
        }
        SCRATCH.with(|scratch| {
            let mut w = Writer {
                out: std::mem::take(&mut *scratch.borrow_mut()),
            };
            w.out.clear();
            let result = BuildValue { w: &mut w }.deserialize(deserializer);
            let out = match result {
                Ok(range) => Ok(FlatVariant::from_bytes(&w.out[range])),
                Err(e) => Err(e),
            };
            *scratch.borrow_mut() = w.out;
            out
        })
    }
}

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for FlatVariant {
    fn deserialize_with_context<D>(
        deserializer: D,
        context: &'de SqlSerdeConfig,
    ) -> Result<FlatVariant, D::Error>
    where
        D: Deserializer<'de>,
    {
        match context.variant_format {
            VariantFormat::Json => FlatVariant::deserialize(deserializer),
            VariantFormat::JsonString => {
                let s = Cow::<String>::deserialize(deserializer)?;
                serde_json::from_str::<FlatVariant>(&s).map_err(|e| {
                    D::Error::custom(format!(
                        "error deserializing VARIANT type from a JSON string: {e}"
                    ))
                })
            }
        }
    }
}

// serde: FlatVariant -> JSON, byte-identical to Variant's output

fn json_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default().with_variant_format(VariantFormat::Json)
}

/// Serializes the encoded value at `bytes`, delegating scalar rendering to
/// the same sqllib serializers `Variant` uses.
struct Enc<'a>(&'a [u8]);

impl Serialize for Enc<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.0;
        let p = &bytes[1..];
        match bytes[0] {
            TAG_SQL_NULL | TAG_VARIANT_NULL => serializer.serialize_none(),
            TAG_BOOLEAN => serializer.serialize_bool(p[0] != 0),
            TAG_TINYINT => serializer.serialize_i8(p[0] as i8),
            TAG_SMALLINT => serializer.serialize_i16(i16::from_le_bytes(p.try_into().unwrap())),
            TAG_INT => serializer.serialize_i32(i32::from_le_bytes(p.try_into().unwrap())),
            TAG_BIGINT => serializer.serialize_i64(i64::from_le_bytes(p.try_into().unwrap())),
            TAG_UTINYINT => serializer.serialize_u8(p[0]),
            TAG_USMALLINT => serializer.serialize_u16(u16::from_le_bytes(p.try_into().unwrap())),
            TAG_UINT => serializer.serialize_u32(u32::from_le_bytes(p.try_into().unwrap())),
            TAG_UBIGINT => serializer.serialize_u64(u64::from_le_bytes(p.try_into().unwrap())),
            TAG_REAL => serializer.serialize_f32(f32::from_le_bytes(p.try_into().unwrap())),
            TAG_DOUBLE => serializer.serialize_f64(f64::from_le_bytes(p.try_into().unwrap())),
            TAG_DECIMAL => {
                DynamicDecimal::new(i128::from_le_bytes(p[..16].try_into().unwrap()), p[16])
                    .serialize_with_context(serializer, &json_config())
            }
            TAG_STRING => serializer.serialize_str(std::str::from_utf8(p).expect("encoded UTF-8")),
            TAG_DATE => Date::from_days(i32::from_le_bytes(p.try_into().unwrap()))
                .serialize_with_context(serializer, &json_config()),
            TAG_TIME => Time::from_nanoseconds(u64::from_le_bytes(p.try_into().unwrap()))
                .serialize_with_context(serializer, &json_config()),
            TAG_TIMESTAMP => {
                Timestamp::from_microseconds(i64::from_le_bytes(p.try_into().unwrap()))
                    .serialize_with_context(serializer, &json_config())
            }
            TAG_TIMESTAMP_TZ => {
                TimestampTz::from_microseconds(i64::from_le_bytes(p.try_into().unwrap()))
                    .serialize_with_context(serializer, &json_config())
            }
            TAG_SHORT_INTERVAL => {
                ShortInterval::from_microseconds(i64::from_le_bytes(p.try_into().unwrap()))
                    .serialize_with_context(serializer, &json_config())
            }
            TAG_LONG_INTERVAL => {
                LongInterval::from_months(i32::from_le_bytes(p.try_into().unwrap()))
                    .serialize_with_context(serializer, &json_config())
            }
            // ByteArray serializes as a JSON array of numbers.
            TAG_BINARY => {
                let mut seq = serializer.serialize_seq(Some(p.len()))?;
                for byte in p {
                    seq.serialize_element(byte)?;
                }
                seq.end()
            }
            TAG_GEOMETRY => crate::GeoPoint::new(
                f64::from_le_bytes(p[..8].try_into().unwrap()),
                f64::from_le_bytes(p[8..].try_into().unwrap()),
            )
            .serialize_with_context(serializer, &json_config()),
            TAG_UUID => Uuid::from_bytes(p.try_into().unwrap())
                .serialize_with_context(serializer, &json_config()),
            TAG_ARRAY => {
                let c = Container::new(bytes);
                let mut seq = serializer.serialize_seq(Some(c.count))?;
                for i in 0..c.count {
                    seq.serialize_element(&Enc(&bytes[c.element(i)]))?;
                }
                seq.end()
            }
            TAG_MAP => {
                let c = Container::new(bytes);
                let mut map = serializer.serialize_map(Some(c.count))?;
                for i in 0..c.count {
                    map.serialize_entry(&Enc(&bytes[c.element(i)]), &Enc(&bytes[c.map_value(i)]))?;
                }
                map.end()
            }
            tag => unreachable!("invalid tag {tag}"),
        }
    }
}

impl Serialize for FlatVariant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Enc(self.as_bytes()).serialize(serializer)
    }
}

impl SerializeWithContext<SqlSerdeConfig> for FlatVariant {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.variant_format {
            VariantFormat::JsonString => {
                serializer.serialize_str(&self.to_json_string().map_err(|e| {
                    serde::ser::Error::custom(format!(
                        "error serializing VARIANT to JSON string: {e}"
                    ))
                })?)
            }
            VariantFormat::Json => self.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn scalar() -> impl Strategy<Value = Variant> {
        prop_oneof![
            Just(Variant::SqlNull),
            Just(Variant::VariantNull),
            any::<bool>().prop_map(Variant::Boolean),
            any::<i8>().prop_map(Variant::TinyInt),
            any::<i64>().prop_map(Variant::BigInt),
            any::<u64>().prop_map(Variant::UBigInt),
            any::<f64>().prop_map(|f| Variant::Double(f.into())),
            (any::<i128>(), any::<u8>()).prop_map(Variant::SqlDecimal),
            ".{0,24}".prop_map(|s| Variant::String(SqlString::from(s))),
            proptest::collection::vec(any::<u8>(), 0..40)
                .prop_map(|b| Variant::Binary(ByteArray::new(&b))),
            any::<[u8; 16]>().prop_map(|b| Variant::Uuid(Uuid::from_bytes(b))),
            (any::<f64>(), any::<f64>())
                .prop_map(|(a, b)| Variant::Geometry(crate::GeoPoint::new(a, b))),
        ]
    }

    fn variant() -> impl Strategy<Value = Variant> {
        scalar().prop_recursive(3, 64, 8, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8)
                    .prop_map(|v| Variant::Array(v.into())),
                proptest::collection::vec((inner.clone(), inner), 0..8).prop_map(|pairs| {
                    Variant::Map(pairs.into_iter().collect::<BTreeMap<_, _>>().into())
                }),
            ]
        })
    }

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut h);
        h.finish()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn ord_matches(a in variant(), b in variant()) {
            let (a2, b2) = (FlatVariant::from(&a), FlatVariant::from(&b));
            prop_assert_eq!(a.cmp(&b), a2.cmp(&b2));
            prop_assert_eq!(a == b, a2 == b2);
        }

        #[test]
        fn hash_consistent(a in variant(), b in variant()) {
            let (a2, b2) = (FlatVariant::from(&a), FlatVariant::from(&b));
            if a2 == b2 {
                prop_assert_eq!(hash_of(&a2), hash_of(&b2));
            }
        }

        #[test]
        fn roundtrip(a in variant()) {
            let a2 = FlatVariant::from(&a);
            prop_assert_eq!(&Variant::from(&a2), &a);
        }

        #[test]
        fn rkyv_roundtrip(a in variant()) {
            use rkyv::Deserialize as _;
            use rkyv::ser::Serializer as _;
            let a2 = FlatVariant::from(&a);
            let mut serializer =
                rkyv::ser::serializers::AllocSerializer::<1024>::default();
            serializer.serialize_value(&a2).unwrap();
            let bytes = serializer.into_serializer().into_inner();
            // SAFETY: bytes come from serializing FlatVariant just above.
            let archived = unsafe { rkyv::archived_root::<FlatVariant>(&bytes) };
            let mut deserializer = rkyv::de::deserializers::SharedDeserializeMap::new();
            let back: FlatVariant = archived.deserialize(&mut deserializer).unwrap();
            prop_assert_eq!(back, a2);
        }

        #[test]
        fn index_matches(a in variant(), key in ".{0,12}") {
            let a2 = FlatVariant::from(&a);
            let old = a.index_string(&key);
            let new = a2.index_string(&key);
            prop_assert_eq!(&Variant::from(&new), &old);
        }
    }

    /// The MAP cast must treat a JSON null member exactly like the enum
    /// path, where `Option<Variant>::try_from` always wraps in `Some`
    /// (MapTests#mapValuesVariant regression).
    #[test]
    fn map_cast_null_member() {
        let json = r#"{"a":"1","d":null}"#;
        let v1: Variant = serde_json::from_str(json).unwrap();
        let v2: FlatVariant = serde_json::from_str(json).unwrap();
        let m1 = crate::casts::cast_to_mapN_VN::<SqlString, Option<Variant>>(Some(v1))
            .unwrap()
            .unwrap();
        let m2 = crate::flat_variant::casts::cast_to_mapN_FVN::<SqlString, Option<FlatVariant>>(
            Some(v2),
        )
        .unwrap()
        .unwrap();
        for key in ["a", "d"] {
            let e1 = m1.get(&SqlString::from_ref(key)).expect("key present");
            let e2 = m2.get(&SqlString::from_ref(key)).expect("key present");
            assert_eq!(
                e1.as_ref().map(FlatVariant::from),
                e2.clone(),
                "MAP element for {key} diverges from the enum path"
            );
        }
    }

    /// Real JSON parses identically through both types and emits identical
    /// output text.
    #[test]
    fn json_differential() {
        let cases = [
            r#"null"#,
            r#"true"#,
            r#"0"#,
            r#"-5"#,
            r#"5"#,
            r#"18446744073709551616"#,
            r#"5.0"#,
            r#"0.1"#,
            r#"123E-5"#,
            r#""hello""#,
            r#"[]"#,
            r#"[1, "two", null, [3.5], {"a": false}]"#,
            r#"{}"#,
            r#"{"b": 1, "a": 2, "b": 3}"#,
            r#"{"nested": {"x": [1, 2, {"deep": "value"}]}}"#,
        ];
        for case in cases {
            let v1: Variant = serde_json::from_str(case).unwrap();
            let v2: FlatVariant = serde_json::from_str(case).unwrap();
            assert_eq!(FlatVariant::from(&v1), v2, "parse mismatch for {case}");
            assert_eq!(
                v1.to_json_string().unwrap(),
                v2.to_json_string().unwrap(),
                "output mismatch for {case}"
            );
        }
    }
}
