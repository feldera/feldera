//! FlatVariant: a flat-buffer implementation of the SQL VARIANT type.
//!
//! One contiguous byte buffer holds an entire VARIANT document; sub-values
//! are ranges into the shared buffer. Compared to [`crate::variant::Variant`]
//! (a recursive enum over `Arc<BTreeMap>`/`Arc<Vec>`), building a value from
//! JSON is a linear buffer write, rkyv serialization is one bulk write,
//! deserialization is one allocation plus a memcpy, drop is a single
//! dealloc, and, because the encoding is canonical (map keys are stored
//! sorted), equality is a memcmp except through float payloads.
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
//! Array:  [count: u32][end_i: u32 x count][children's encodings, concatenated]
//! Map:    [count: u32][key_end_i: u32 x count][val_end_i: u32 x count]
//!         [keys' encodings, concatenated][values' encodings, concatenated]
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
pub mod functions;

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

// The numeric tag order defines the type rank used by Ord and the sorted
// map-key order, and it matches the Variant enum's discriminant order so
// the two representations sort identically. Reordering or renumbering tags
// changes the persisted encoding.
pub(crate) const TAG_SQL_NULL: u8 = 0;
pub(crate) const TAG_VARIANT_NULL: u8 = 1;
pub(crate) const TAG_BOOLEAN: u8 = 2;
pub(crate) const TAG_TINYINT: u8 = 3;
pub(crate) const TAG_SMALLINT: u8 = 4;
pub(crate) const TAG_INT: u8 = 5;
pub(crate) const TAG_BIGINT: u8 = 6;
pub(crate) const TAG_UTINYINT: u8 = 7;
pub(crate) const TAG_USMALLINT: u8 = 8;
pub(crate) const TAG_UINT: u8 = 9;
pub(crate) const TAG_UBIGINT: u8 = 10;
pub(crate) const TAG_REAL: u8 = 11;
pub(crate) const TAG_DOUBLE: u8 = 12;
pub(crate) const TAG_DECIMAL: u8 = 13;
pub(crate) const TAG_STRING: u8 = 14;
pub(crate) const TAG_DATE: u8 = 15;
pub(crate) const TAG_TIME: u8 = 16;
pub(crate) const TAG_TIMESTAMP: u8 = 17;
pub(crate) const TAG_SHORT_INTERVAL: u8 = 18;
pub(crate) const TAG_LONG_INTERVAL: u8 = 19;
pub(crate) const TAG_BINARY: u8 = 20;
pub(crate) const TAG_GEOMETRY: u8 = 21;
pub(crate) const TAG_UUID: u8 = 22;
pub(crate) const TAG_ARRAY: u8 = 23;
pub(crate) const TAG_MAP: u8 = 24;
pub(crate) const TAG_TIMESTAMP_TZ: u8 = 25;

// The type

/// A SQL VARIANT value stored as one flat, canonically encoded byte buffer.
///
/// Cloning bumps a refcount; sub-value access (`index_from_one`,
/// `index_string`)
/// returns views into the shared buffer without copying the subtree.
///
/// # Examples
///
/// Values are built from JSON or from native values via `From`:
///
/// ```
/// use feldera_sqllib::FlatVariant;
///
/// let doc: FlatVariant = serde_json::from_str(r#"{"user": {"id": 5}}"#).unwrap();
/// assert_eq!(doc.index_string("user").index_string("id"), FlatVariant::from(5u64));
/// assert_eq!(doc.to_json_string().unwrap(), r#"{"user":{"id":5}}"#);
/// ```
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
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        assert!(!bytes.is_empty(), "encoded value cannot be empty");
        FlatVariant {
            buf: Arc::from(bytes),
            start: 0,
            len: bytes.len().try_into().expect("no more than 4 GB of data"),
        }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start as usize..(self.start + self.len) as usize]
    }

    /// A sub-value sharing this value's buffer; `range` is relative to
    /// `self.as_bytes()`.
    fn subvalue(&self, range: Range<usize>) -> FlatVariant {
        debug_assert!(range.start < self.len as usize);
        debug_assert!(range.end <= self.len as usize);
        debug_assert!(range.end > range.start);
        FlatVariant {
            buf: self.buf.clone(),
            start: self.start + range.start as u32,
            len: (range.end - range.start) as u32,
        }
    }

    /// The SQL NULL value (`Variant::SqlNull` equivalent).
    ///
    /// # Examples
    ///
    /// ```
    /// use feldera_sqllib::FlatVariant;
    ///
    /// assert_eq!(FlatVariant::sql_null(), FlatVariant::default());
    /// assert_eq!(FlatVariant::sql_null().to_json_string().unwrap(), "null");
    /// ```
    pub fn sql_null() -> FlatVariant {
        static NULL: OnceLock<Arc<[u8]>> = OnceLock::new();
        let buf = NULL.get_or_init(|| Arc::from([TAG_SQL_NULL])).clone();
        FlatVariant {
            buf,
            start: 0,
            len: 1,
        }
    }

    /// The JSON null value (`Variant::VariantNull` equivalent).
    ///
    /// # Examples
    ///
    /// A JSON null is a value of its own, distinct from the SQL NULL:
    ///
    /// ```
    /// use feldera_sqllib::FlatVariant;
    ///
    /// let null: FlatVariant = serde_json::from_str("null").unwrap();
    /// assert_eq!(null, FlatVariant::variant_null());
    /// assert_ne!(null, FlatVariant::sql_null());
    /// ```
    pub fn variant_null() -> FlatVariant {
        static NULL: OnceLock<Arc<[u8]>> = OnceLock::new();
        let buf = NULL.get_or_init(|| Arc::from([TAG_VARIANT_NULL])).clone();
        FlatVariant {
            buf,
            start: 0,
            len: 1,
        }
    }

    /// Map lookup by string key; missing key or non-map yields SqlNull.
    /// Same semantics as `Variant::index_string`, but the result shares the
    /// buffer instead of cloning the subtree.
    ///
    /// # Examples
    ///
    /// ```
    /// use feldera_sqllib::FlatVariant;
    ///
    /// let doc: FlatVariant = serde_json::from_str(r#"{"a": 1}"#).unwrap();
    /// assert_eq!(doc.index_string("a"), FlatVariant::from(1u64));
    /// assert_eq!(doc.index_string("missing"), FlatVariant::sql_null());
    /// ```
    pub fn index_string<I: AsRef<str>>(&self, index: I) -> FlatVariant {
        let bytes = self.as_bytes();
        if bytes[0] != TAG_MAP {
            return FlatVariant::sql_null();
        }
        let key = index.as_ref();
        match self.find_key_by(|encoded| cmp_with_string_key(encoded, key)) {
            Some(i) => self.subvalue(Container::new(bytes).map_value(i)),
            None => FlatVariant::sql_null(),
        }
    }

    /// VARIANT indexing: 1-based array access with numeric key coercion,
    /// map lookup by exact key, None otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use feldera_sqllib::FlatVariant;
    ///
    /// let arr: FlatVariant = serde_json::from_str("[10, 20, 30]").unwrap();
    /// // SQL array indexes start at 1.
    /// assert_eq!(arr.index_from_one(&FlatVariant::from(1i32)), Some(FlatVariant::from(10u64)));
    /// assert_eq!(arr.index_from_one(&FlatVariant::from(9i32)), None);
    /// ```
    pub fn index_from_one(&self, index: &FlatVariant) -> Option<FlatVariant> {
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
        self.find_key_by(|encoded| cmp_values(encoded, probe))
    }

    /// Binary search the sorted key area of a map with `cmp`, which compares
    /// an encoded key against the probe.
    fn find_key_by(&self, cmp: impl Fn(&[u8]) -> Ordering) -> Option<usize> {
        let bytes = self.as_bytes();
        let c = Container::new(bytes);
        let mut lo = 0usize;
        let mut hi = c.count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            match cmp(&bytes[c.element(mid)]) {
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
            TAG_SMALLINT => i16::from_le_bytes(payload_array(p)) as isize,
            TAG_INT => i32::from_le_bytes(payload_array(p)) as isize,
            TAG_BIGINT => i64::from_le_bytes(payload_array(p)) as isize,
            TAG_UTINYINT => p[0] as isize,
            TAG_USMALLINT => u16::from_le_bytes(payload_array(p)) as isize,
            TAG_UINT => u32::from_le_bytes(payload_array(p)) as isize,
            TAG_UBIGINT => isize::try_from(u64::from_le_bytes(payload_array(p))).ok()?,
            _ => return None,
        })
    }

    /// The canonical JSON text of this value; same output as
    /// `Variant::to_json_string`.
    ///
    /// # Examples
    ///
    /// ```
    /// use feldera_sqllib::FlatVariant;
    ///
    /// let doc: FlatVariant = serde_json::from_str(r#"{ "b" : 1, "a" : [true] }"#).unwrap();
    /// assert_eq!(doc.to_json_string().unwrap(), r#"{"a":[true],"b":1}"#);
    /// ```
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
        // The Arc impl deduplicates by pointer, so sub-values sharing one
        // document buffer count it once per context.
        self.buf.size_of_children(context);
    }
}

// Container access

/// Read access to one encoded array or map: the child count, the range of
/// each element or key, and the range of each map value, all resolved
/// through the end-offset tables in O(1) per child.
pub(crate) struct Container<'a> {
    body: &'a [u8],
    pub(crate) count: usize,
    is_map: bool,
}

#[inline]
fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(payload_array(&bytes[at..at + 4]))
}

impl<'a> Container<'a> {
    /// `value` is one complete encoded value with an Array or Map tag.
    pub(crate) fn new(value: &'a [u8]) -> Self {
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
    pub(crate) fn element(&self, i: usize) -> Range<usize> {
        debug_assert!(i < self.count);
        let base = self.payload_base();
        let start = if i == 0 { 0 } else { self.end(0, i - 1) };
        // +1 converts from body-relative to value-relative.
        1 + base + start..1 + base + self.end(0, i)
    }

    /// Range of map value `i`, relative to the start of the complete value.
    pub(crate) fn map_value(&self, i: usize) -> Range<usize> {
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
    F32::new(f32::from_le_bytes(payload_array(&b[at..at + 4])))
}

#[inline]
fn f64_at(b: &[u8], at: usize) -> F64 {
    F64::new(f64::from_le_bytes(payload_array(&b[at..at + 8])))
}

/// `cmp_values` against a string key without materializing the key's
/// encoding: tag rank first, then payload bytes (str Ord is bytewise).
fn cmp_with_string_key(encoded: &[u8], key: &str) -> Ordering {
    match encoded[0].cmp(&TAG_STRING) {
        Ordering::Equal => encoded[1..].cmp(key.as_bytes()),
        rank => rank,
    }
}

/// Total order over two complete encoded values: tag rank first, then
/// payload. This is the storage order backing Ord, sorted map keys, and
/// map lookup, not SQL comparison semantics: decimals order
/// lexicographically by (significand, scale) rather than numerically, and
/// floats use the F32/F64 total order.
pub(crate) fn cmp_values(a: &[u8], b: &[u8]) -> Ordering {
    let (ta, tb) = (a[0], b[0]);
    if ta != tb {
        return ta.cmp(&tb);
    }
    let (pa, pb) = (&a[1..], &b[1..]);
    match ta {
        TAG_SQL_NULL | TAG_VARIANT_NULL => Ordering::Equal,
        TAG_TINYINT => (pa[0] as i8).cmp(&(pb[0] as i8)),
        TAG_SMALLINT => {
            i16::from_le_bytes(payload_array(pa)).cmp(&i16::from_le_bytes(payload_array(pb)))
        }
        TAG_INT | TAG_DATE | TAG_LONG_INTERVAL => {
            i32::from_le_bytes(payload_array(pa)).cmp(&i32::from_le_bytes(payload_array(pb)))
        }
        TAG_BIGINT | TAG_TIMESTAMP | TAG_SHORT_INTERVAL | TAG_TIMESTAMP_TZ => {
            i64::from_le_bytes(payload_array(pa)).cmp(&i64::from_le_bytes(payload_array(pb)))
        }
        TAG_BOOLEAN | TAG_UTINYINT => pa[0].cmp(&pb[0]),
        TAG_USMALLINT => {
            u16::from_le_bytes(payload_array(pa)).cmp(&u16::from_le_bytes(payload_array(pb)))
        }
        TAG_UINT => {
            u32::from_le_bytes(payload_array(pa)).cmp(&u32::from_le_bytes(payload_array(pb)))
        }
        TAG_UBIGINT | TAG_TIME => {
            u64::from_le_bytes(payload_array(pa)).cmp(&u64::from_le_bytes(payload_array(pb)))
        }
        TAG_REAL => f32_at(pa, 0).cmp(&f32_at(pb, 0)),
        TAG_DOUBLE => f64_at(pa, 0).cmp(&f64_at(pb, 0)),
        TAG_GEOMETRY => (f64_at(pa, 0), f64_at(pa, 8)).cmp(&(f64_at(pb, 0), f64_at(pb, 8))),
        TAG_DECIMAL => {
            let da = i128::from_le_bytes(payload_array(&pa[..16]));
            let db = i128::from_le_bytes(payload_array(&pb[..16]));
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

/// Equality of two complete encoded values.
fn eq_values(a: &[u8], b: &[u8]) -> bool {
    // The encoding is canonical, so equal bytes mean equal values. The
    // reverse fails only through float payloads (negative zero and NaN
    // compare equal across different bit patterns), hence the structural
    // fallback.
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

/// Appends encoded values to a scratch buffer.
///
/// A container whose child count is known upfront is encoded in place:
/// the header is reserved first and the end-offset tables are backpatched
/// as the children land directly behind it (`array_in_place`,
/// `begin_map_in_place`). When the count is unknown (serde without a size
/// hint) or the entries need sorting, children are encoded first anywhere
/// in the scratch and the container is assembled after them with
/// `extend_from_within`, copying bytes O(depth) times.
///
/// Public because it appears in `EncodeFV::encode`; not part of the
/// supported API.
#[doc(hidden)]
pub struct Writer {
    pub(crate) out: Vec<u8>,
}

/// Checked conversion for every count and end offset written into a
/// container's u32 tables; a wrap here would silently corrupt the encoding.
#[inline]
fn encoded_u32(value: usize) -> u32 {
    value.try_into().expect("no more than 4 GB of data")
}

/// Fixed-width payload accessor: the tag determines the payload width, so
/// the conversion cannot fail on a well-formed encoding.
#[inline]
pub(crate) fn payload_array<const N: usize>(payload: &[u8]) -> [u8; N] {
    payload.try_into().expect("payload width matches the tag")
}

/// A reserved, zero-filled end-offset table inside a [`Writer`], backpatched
/// one child at a time.
pub(crate) struct EndTable {
    /// Byte position of the table in the writer.
    table: usize,
    /// Byte position where this table's payload area starts; for a map's
    /// value table this is only known after the keys, so it is set late.
    payload_start: usize,
    /// Index of the next child to record.
    next: usize,
}

impl EndTable {
    /// Record that the next child's encoding ends at the writer's current
    /// position.
    pub(crate) fn record_end(&mut self, w: &mut Writer) {
        debug_assert!(
            self.payload_start <= w.out.len(),
            "value table used before begin_map_values"
        );
        let end = encoded_u32(w.out.len() - self.payload_start);
        let slot = self.table + 4 * self.next;
        w.out[slot..slot + 4].copy_from_slice(&end.to_le_bytes());
        self.next += 1;
    }
}

impl Writer {
    #[inline]
    pub(crate) fn scalar(&mut self, tag: u8, payload: &[u8]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(tag);
        self.out.extend_from_slice(payload);
        start..self.out.len()
    }

    /// Append a complete, already encoded value verbatim.
    #[inline]
    pub(crate) fn raw(&mut self, value: &[u8]) -> Range<usize> {
        let start = self.out.len();
        self.out.extend_from_slice(value);
        start..self.out.len()
    }

    /// Reserve a zero-filled end-offset table for `count` children.
    fn reserve_table(&mut self, count: usize) -> usize {
        let table = self.out.len();
        self.out.resize(table + 4 * count, 0);
        table
    }

    /// Encode an array of `count` children in place: `encode_child(w, i)`
    /// appends the i-th child, and the end-offset table is backpatched as
    /// the children land behind the header, with no copying.
    pub(crate) fn array_in_place(
        &mut self,
        count: usize,
        mut encode_child: impl FnMut(&mut Writer, usize),
    ) -> Range<usize> {
        let start = self.out.len();
        self.out.push(TAG_ARRAY);
        self.out
            .extend_from_slice(&encoded_u32(count).to_le_bytes());
        let table = self.reserve_table(count);
        let mut ends = EndTable {
            table,
            payload_start: self.out.len(),
            next: 0,
        };
        for i in 0..count {
            encode_child(self, i);
            ends.record_end(self);
        }
        start..self.out.len()
    }

    /// Encode a map of `count` entries in place. The caller encodes all keys
    /// first, then all values, calling `record_end` after each; keys must
    /// arrive sorted ascending by encoded key with no duplicates (the map
    /// layout stores the key and value areas separately). Returns the two
    /// end tables and the container's start.
    pub(crate) fn begin_map_in_place(&mut self, count: usize) -> (usize, EndTable, EndTable) {
        let start = self.out.len();
        self.out.push(TAG_MAP);
        self.out
            .extend_from_slice(&encoded_u32(count).to_le_bytes());
        let key_table = self.reserve_table(count);
        let val_table = self.reserve_table(count);
        let payload_start = self.out.len();
        (
            start,
            EndTable {
                table: key_table,
                payload_start,
                next: 0,
            },
            EndTable {
                table: val_table,
                // The value area starts after the keys; fixed up by
                // `begin_map_values`.
                payload_start: usize::MAX,
                next: 0,
            },
        )
    }

    /// Mark the end of the key area of an in-place map: values encoded from
    /// here on are recorded relative to the current position.
    pub(crate) fn begin_map_values(&mut self, val_ends: &mut EndTable) {
        val_ends.payload_start = self.out.len();
    }

    /// Assemble an array from child value ranges within this writer.
    pub(crate) fn array(&mut self, children: &[Range<usize>]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(TAG_ARRAY);
        self.out
            .extend_from_slice(&encoded_u32(children.len()).to_le_bytes());
        let mut end = 0usize;
        for r in children {
            end += r.len();
            self.out.extend_from_slice(&encoded_u32(end).to_le_bytes());
        }
        for r in children {
            self.out.extend_from_within(r.clone());
        }
        start..self.out.len()
    }

    /// Assemble a map from (key, value) ranges within this writer. Entries
    /// must be sorted ascending by encoded key with no duplicates.
    pub(crate) fn map(&mut self, entries: &[(Range<usize>, Range<usize>)]) -> Range<usize> {
        let start = self.out.len();
        self.out.push(TAG_MAP);
        self.out
            .extend_from_slice(&encoded_u32(entries.len()).to_le_bytes());
        let mut end = 0usize;
        for (k, _) in entries {
            end += k.len();
            self.out.extend_from_slice(&encoded_u32(end).to_le_bytes());
        }
        let mut end = 0usize;
        for (_, v) in entries {
            end += v.len();
            self.out.extend_from_slice(&encoded_u32(end).to_le_bytes());
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
pub(crate) fn sort_map_entries(out: &[u8], entries: &mut Vec<(Range<usize>, Range<usize>)>) {
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
            w.scalar(TAG_DECIMAL, &casts::decimal_payload(*sig, *scale))
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
        Variant::Array(items) => w.array_in_place(items.len(), |w, i| {
            encode_variant(w, &items[i]);
        }),
        Variant::Map(map) => {
            // BTreeMap iterates in Variant Ord order, which equals
            // cmp_values order on the encodings: sorted and deduplicated.
            let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(map.len());
            for k in map.keys() {
                encode_variant(w, k);
                key_ends.record_end(w);
            }
            w.begin_map_values(&mut val_ends);
            for val in map.values() {
                encode_variant(w, val);
                val_ends.record_end(w);
            }
            start..w.out.len()
        }
    }
}

/// Run `enc` against a per-thread scratch writer and copy the returned
/// range into a right-sized document. Document construction (serde, enum
/// conversion, casts) funnels through here to reuse the scratch buffer.
/// A reentrant call takes a fresh buffer and only misses the reuse.
pub(crate) fn build_document<E>(
    enc: impl FnOnce(&mut Writer) -> Result<Range<usize>, E>,
) -> Result<FlatVariant, E> {
    thread_local! {
        static SCRATCH: std::cell::RefCell<Vec<u8>> =
            std::cell::RefCell::new(Vec::with_capacity(4096));
    }
    // Don't let one huge document pin its buffer in the thread-local
    // forever; oversized scratches shrink back to this cap.
    const SCRATCH_RETAIN_BYTES: usize = 2 << 20;
    SCRATCH.with(|scratch| {
        let mut w = Writer {
            out: std::mem::take(&mut *scratch.borrow_mut()),
        };
        w.out.clear();
        let result = enc(&mut w);
        let out = result.map(|range| FlatVariant::from_bytes(&w.out[range]));
        if w.out.capacity() > SCRATCH_RETAIN_BYTES {
            w.out.clear();
            w.out.shrink_to(SCRATCH_RETAIN_BYTES);
        }
        *scratch.borrow_mut() = w.out;
        out
    })
}

/// [`build_document`] for encoders that cannot fail.
pub(crate) fn build_document_infallible(
    enc: impl FnOnce(&mut Writer) -> Range<usize>,
) -> FlatVariant {
    match build_document::<std::convert::Infallible>(|w| Ok(enc(w))) {
        Ok(document) => document,
    }
}

/// The connector-metadata boundary: the adapters always build metadata as
/// the enum `Variant`, and a metadata DEFAULT expression for a FlatVariant
/// column converts it once here. Goes away with the enum.
impl From<&Variant> for FlatVariant {
    fn from(v: &Variant) -> Self {
        build_document_infallible(|w| encode_variant(w, v))
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
        TAG_SMALLINT => Variant::SmallInt(i16::from_le_bytes(payload_array(payload))),
        TAG_INT => Variant::Int(i32::from_le_bytes(payload_array(payload))),
        TAG_BIGINT => Variant::BigInt(i64::from_le_bytes(payload_array(payload))),
        TAG_UTINYINT => Variant::UTinyInt(payload[0]),
        TAG_USMALLINT => Variant::USmallInt(u16::from_le_bytes(payload_array(payload))),
        TAG_UINT => Variant::UInt(u32::from_le_bytes(payload_array(payload))),
        TAG_UBIGINT => Variant::UBigInt(u64::from_le_bytes(payload_array(payload))),
        TAG_REAL => Variant::Real(f32::from_le_bytes(payload_array(payload)).into()),
        TAG_DOUBLE => Variant::Double(f64::from_le_bytes(payload_array(payload)).into()),
        TAG_DECIMAL => Variant::SqlDecimal((
            i128::from_le_bytes(payload_array(&payload[..16])),
            payload[16],
        )),
        TAG_STRING => Variant::String(SqlString::from_ref(
            std::str::from_utf8(payload).expect("encoded string must be UTF-8"),
        )),
        TAG_DATE => Variant::Date(Date::from_days(i32::from_le_bytes(payload_array(payload)))),
        TAG_TIME => Variant::Time(Time::from_nanoseconds(u64::from_le_bytes(payload_array(
            payload,
        )))),
        TAG_TIMESTAMP => Variant::Timestamp(Timestamp::from_microseconds(i64::from_le_bytes(
            payload_array(payload),
        ))),
        TAG_TIMESTAMP_TZ => Variant::TimestampTz(TimestampTz::from_microseconds(
            i64::from_le_bytes(payload_array(payload)),
        )),
        TAG_SHORT_INTERVAL => Variant::ShortInterval(ShortInterval::from_microseconds(
            i64::from_le_bytes(payload_array(payload)),
        )),
        TAG_LONG_INTERVAL => Variant::LongInterval(LongInterval::from_months(i32::from_le_bytes(
            payload_array(payload),
        ))),
        TAG_BINARY => Variant::Binary(ByteArray::new(payload)),
        TAG_GEOMETRY => Variant::Geometry(crate::GeoPoint::new(
            f64::from_le_bytes(payload_array(&payload[..8])),
            f64::from_le_bytes(payload_array(&payload[8..])),
        )),
        TAG_UUID => Variant::Uuid(Uuid::from_bytes(payload_array(payload))),
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

#[cfg(test)]
impl From<FlatVariant> for Variant {
    fn from(v: FlatVariant) -> Self {
        Variant::from(&v)
    }
}

// The connector-metadata boundary: metadata is always built by the adapters
// as the enum Variant, and a metadata DEFAULT expression for a FlatVariant
// VARIANT column converts once here. The only sanctioned production use of
// an enum-to-flat conversion.
impl From<Variant> for FlatVariant {
    fn from(v: Variant) -> Self {
        FlatVariant::from(&v)
    }
}

/// See [`From<Variant>`]: the connector-metadata boundary conversion for a
/// nullable VARIANT column with a metadata DEFAULT.
#[doc(hidden)]
pub fn variant_to_fvN(v: Option<Variant>) -> Option<FlatVariant> {
    v.map(|v| FlatVariant::from(&v))
}

/// See [`From<Variant>`]: the non-null variant of the boundary conversion.
#[doc(hidden)]
pub fn variant_to_fv(v: Variant) -> FlatVariant {
    FlatVariant::from(&v)
}

// TryFrom<Variant> for FlatVariant comes from core's blanket over the
// From<Variant> boundary conversion above.

// A VARIANT container element is always `Some`: a JSON null inside a map
// or array stays a variant-null VALUE (MapTests#mapValuesVariant depends
// on this); it does not become a Rust `None`. Test-only, like the other
// bridges.
#[cfg(test)]
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
                crate::flat_variant::casts::encode_document(&value)
            }
        }
        impl From<Option<$t>> for FlatVariant {
            fn from(value: Option<$t>) -> Self {
                crate::flat_variant::casts::encode_document(&value)
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
        crate::flat_variant::casts::encode_document(&value)
    }
}

impl<const P: usize, const S: usize> From<Option<crate::SqlDecimal<P, S>>> for FlatVariant {
    fn from(value: Option<crate::SqlDecimal<P, S>>) -> Self {
        crate::flat_variant::casts::encode_document(&value)
    }
}

impl<T: crate::flat_variant::casts::EncodeFV> From<crate::Array<T>> for FlatVariant {
    fn from(value: crate::Array<T>) -> Self {
        crate::flat_variant::casts::encode_document(&value)
    }
}

impl<T: crate::flat_variant::casts::EncodeFV> From<Option<crate::Array<T>>> for FlatVariant {
    fn from(value: Option<crate::Array<T>>) -> Self {
        crate::flat_variant::casts::encode_document(&value)
    }
}

impl<K: crate::flat_variant::casts::EncodeFV, V: crate::flat_variant::casts::EncodeFV>
    From<crate::Map<K, V>> for FlatVariant
{
    fn from(value: crate::Map<K, V>) -> Self {
        crate::flat_variant::casts::encode_document(&value)
    }
}

impl<K: crate::flat_variant::casts::EncodeFV, V: crate::flat_variant::casts::EncodeFV>
    From<Option<crate::Map<K, V>>> for FlatVariant
{
    fn from(value: Option<crate::Map<K, V>>) -> Self {
        crate::flat_variant::casts::encode_document(&value)
    }
}

// rkyv: the archived form is the encoding itself

/// Archived form of [`FlatVariant`]: the encoded bytes, verbatim.
pub struct ArchivedFlatVariant {
    bytes: ArchivedVec<u8>,
}

impl ArchivedFlatVariant {
    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
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

/// serde visitor that appends one complete encoded value to the writer,
/// yielding its range. Handles serde_json's arbitrary-precision numbers,
/// which arrive as a single-entry map with a private marker key.
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
        Ok(self
            .w
            .scalar(TAG_DECIMAL, &casts::decimal_payload(value, 0)))
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
                    &casts::decimal_payload(number.significand(), number.exponent()),
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
        build_document(|w| BuildValue { w }.deserialize(deserializer))
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

/// Serializes the encoded value at `bytes` through the sqllib scalar
/// serializers. `config` selects the scalar sub-formats (decimal, binary,
/// timestamp, ...) and is threaded into every nested scalar; output
/// connectors with non-default sub-formats depend on this.
struct Enc<'a> {
    bytes: &'a [u8],
    config: &'a SqlSerdeConfig,
}

impl<'a> Enc<'a> {
    fn child(&self, bytes: &'a [u8]) -> Enc<'a> {
        Enc {
            bytes,
            config: self.config,
        }
    }
}

impl Serialize for Enc<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.bytes;
        let p = &bytes[1..];
        match bytes[0] {
            TAG_SQL_NULL | TAG_VARIANT_NULL => serializer.serialize_none(),
            TAG_BOOLEAN => serializer.serialize_bool(p[0] != 0),
            TAG_TINYINT => serializer.serialize_i8(p[0] as i8),
            TAG_SMALLINT => serializer.serialize_i16(i16::from_le_bytes(payload_array(p))),
            TAG_INT => serializer.serialize_i32(i32::from_le_bytes(payload_array(p))),
            TAG_BIGINT => serializer.serialize_i64(i64::from_le_bytes(payload_array(p))),
            TAG_UTINYINT => serializer.serialize_u8(p[0]),
            TAG_USMALLINT => serializer.serialize_u16(u16::from_le_bytes(payload_array(p))),
            TAG_UINT => serializer.serialize_u32(u32::from_le_bytes(payload_array(p))),
            TAG_UBIGINT => serializer.serialize_u64(u64::from_le_bytes(payload_array(p))),
            TAG_REAL => serializer.serialize_f32(f32::from_le_bytes(payload_array(p))),
            TAG_DOUBLE => serializer.serialize_f64(f64::from_le_bytes(payload_array(p))),
            TAG_DECIMAL => DynamicDecimal::new(i128::from_le_bytes(payload_array(&p[..16])), p[16])
                .serialize_with_context(serializer, self.config),
            TAG_STRING => serializer.serialize_str(std::str::from_utf8(p).expect("encoded UTF-8")),
            TAG_DATE => Date::from_days(i32::from_le_bytes(payload_array(p)))
                .serialize_with_context(serializer, self.config),
            TAG_TIME => Time::from_nanoseconds(u64::from_le_bytes(payload_array(p)))
                .serialize_with_context(serializer, self.config),
            TAG_TIMESTAMP => Timestamp::from_microseconds(i64::from_le_bytes(payload_array(p)))
                .serialize_with_context(serializer, self.config),
            TAG_TIMESTAMP_TZ => {
                TimestampTz::from_microseconds(i64::from_le_bytes(payload_array(p)))
                    .serialize_with_context(serializer, self.config)
            }
            TAG_SHORT_INTERVAL => {
                ShortInterval::from_microseconds(i64::from_le_bytes(payload_array(p)))
                    .serialize_with_context(serializer, self.config)
            }
            TAG_LONG_INTERVAL => LongInterval::from_months(i32::from_le_bytes(payload_array(p)))
                .serialize_with_context(serializer, self.config),
            // ByteArray honors the config's binary format.
            TAG_BINARY => ByteArray::new(p).serialize_with_context(serializer, self.config),
            TAG_GEOMETRY => crate::GeoPoint::new(
                f64::from_le_bytes(payload_array(&p[..8])),
                f64::from_le_bytes(payload_array(&p[8..])),
            )
            .serialize_with_context(serializer, self.config),
            TAG_UUID => {
                Uuid::from_bytes(payload_array(p)).serialize_with_context(serializer, self.config)
            }
            TAG_ARRAY => {
                let c = Container::new(bytes);
                let mut seq = serializer.serialize_seq(Some(c.count))?;
                for i in 0..c.count {
                    seq.serialize_element(&self.child(&bytes[c.element(i)]))?;
                }
                seq.end()
            }
            TAG_MAP => {
                let c = Container::new(bytes);
                let mut map = serializer.serialize_map(Some(c.count))?;
                for i in 0..c.count {
                    map.serialize_entry(
                        &self.child(&bytes[c.element(i)]),
                        &self.child(&bytes[c.map_value(i)]),
                    )?;
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
        let config = json_config();
        Enc {
            bytes: self.as_bytes(),
            config: &config,
        }
        .serialize(serializer)
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
            VariantFormat::Json => Enc {
                bytes: self.as_bytes(),
                config: context,
            }
            .serialize(serializer),
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
            any::<i16>().prop_map(Variant::SmallInt),
            any::<i32>().prop_map(Variant::Int),
            any::<i64>().prop_map(Variant::BigInt),
            any::<u8>().prop_map(Variant::UTinyInt),
            any::<u16>().prop_map(Variant::USmallInt),
            any::<u32>().prop_map(Variant::UInt),
            any::<u64>().prop_map(Variant::UBigInt),
            any::<f32>().prop_map(|f| Variant::Real(f.into())),
            any::<f64>().prop_map(|f| Variant::Double(f.into())),
            // DynamicDecimal panics on unrepresentable (significand, scale)
            // pairs in both grids; stay within its domain.
            (
                -1_000_000_000_000_000_000_000_000_000i128
                    ..1_000_000_000_000_000_000_000_000_000i128,
                0u8..30
            )
                .prop_map(Variant::SqlDecimal),
            ".{0,24}".prop_map(|s| Variant::String(SqlString::from(s))),
            // Ranges renderable by chrono; string casts format these.
            (-100_000i32..100_000).prop_map(|d| Variant::Date(Date::from_days(d))),
            (0u64..86_400_000_000_000).prop_map(|n| Variant::Time(Time::from_nanoseconds(n))),
            (-4_000_000_000_000_000i64..4_000_000_000_000_000)
                .prop_map(|us| Variant::Timestamp(Timestamp::from_microseconds(us))),
            (-4_000_000_000_000_000i64..4_000_000_000_000_000)
                .prop_map(|us| Variant::TimestampTz(TimestampTz::from_microseconds(us))),
            (-1_000_000_000_000_000i64..1_000_000_000_000_000)
                .prop_map(|us| Variant::ShortInterval(ShortInterval::from_microseconds(us))),
            (-1_000_000i32..1_000_000)
                .prop_map(|m| Variant::LongInterval(LongInterval::from_months(m))),
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

    /// Compares one nullable-result from-variant cast pair on one value:
    /// same Ok/Err status and equal values (error text may differ).
    fn check_from<T: PartialEq + std::fmt::Debug>(
        a: &Variant,
        a2: &FlatVariant,
        enum_cast: impl Fn(Variant) -> crate::error::SqlResult<Option<T>>,
        flat_cast: impl Fn(FlatVariant) -> crate::error::SqlResult<Option<T>>,
        what: &str,
    ) {
        assert_eq!(
            enum_cast(a.clone()).map_err(|_| ()),
            flat_cast(a2.clone()).map_err(|_| ()),
            "{what} cast diverges for {a:?}"
        );
    }

    /// Every from-variant cast in the native grid must agree with the enum
    /// grid, over the full range of encoded values.
    #[test]
    fn casts_match_enum_grid() {
        use crate::casts as c1;
        use crate::flat_variant::casts as c2;
        use crate::flat_variant::functions as f2;
        use proptest::strategy::ValueTree;

        /// Interval unit names share one implementation per underlying type;
        /// still call every generated name once.
        macro_rules! check_interval_units {
            ($a:expr, $a2:expr, $($name:ident),* $(,)?) => {::paste::paste! {$(
                check_from($a, $a2,
                    c1::[<cast_to_ $name N_V>], c2::[<cast_to_ $name N_FV>],
                    stringify!($name));
            )*}};
        }

        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let strategy = variant();
        for _ in 0..500 {
            let a = strategy.new_tree(&mut runner).expect("strategy").current();
            let a2 = FlatVariant::from(&a);

            check_from(&a, &a2, c1::cast_to_bN_V, c2::cast_to_bN_FV, "bool");
            check_from(&a, &a2, c1::cast_to_i8N_V, c2::cast_to_i8N_FV, "i8");
            check_from(&a, &a2, c1::cast_to_i16N_V, c2::cast_to_i16N_FV, "i16");
            check_from(&a, &a2, c1::cast_to_i32N_V, c2::cast_to_i32N_FV, "i32");
            check_from(&a, &a2, c1::cast_to_i64N_V, c2::cast_to_i64N_FV, "i64");
            check_from(&a, &a2, c1::cast_to_u8N_V, c2::cast_to_u8N_FV, "u8");
            check_from(&a, &a2, c1::cast_to_u16N_V, c2::cast_to_u16N_FV, "u16");
            check_from(&a, &a2, c1::cast_to_u32N_V, c2::cast_to_u32N_FV, "u32");
            check_from(&a, &a2, c1::cast_to_u64N_V, c2::cast_to_u64N_FV, "u64");
            check_from(&a, &a2, c1::cast_to_fN_V, c2::cast_to_fN_FV, "real");
            check_from(&a, &a2, c1::cast_to_dN_V, c2::cast_to_dN_FV, "double");
            check_from(
                &a,
                &a2,
                |v| c1::cast_to_sN_V(v, -1, false),
                |v| c2::cast_to_sN_FV(v, -1, false),
                "varchar",
            );
            check_from(
                &a,
                &a2,
                |v| c1::cast_to_sN_V(v, 3, true),
                |v| c2::cast_to_sN_FV(v, 3, true),
                "char(3)",
            );
            check_from(
                &a,
                &a2,
                |v| c1::cast_to_bytesN_V(v, -1, false),
                |v| c2::cast_to_bytesN_FV(v, -1, false),
                "binary",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_SqlDecimalN_V::<10, 2>,
                c2::cast_to_SqlDecimalN_FV::<10, 2>,
                "decimal(10,2)",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_SqlDecimalN_V::<38, 10>,
                c2::cast_to_SqlDecimalN_FV::<38, 10>,
                "decimal(38,10)",
            );
            check_from(&a, &a2, c1::cast_to_DateN_V, c2::cast_to_DateN_FV, "date");
            check_from(&a, &a2, c1::cast_to_TimeN_V, c2::cast_to_TimeN_FV, "time");
            check_from(
                &a,
                &a2,
                c1::cast_to_TimestampN_V,
                c2::cast_to_TimestampN_FV,
                "timestamp",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_TimestampTzN_V,
                c2::cast_to_TimestampTzN_FV,
                "timestamptz",
            );
            check_from(&a, &a2, c1::cast_to_UuidN_V, c2::cast_to_UuidN_FV, "uuid");
            check_from(
                &a,
                &a2,
                c1::cast_to_GeoPointN_V,
                c2::cast_to_GeoPointN_FV,
                "geopoint",
            );
            check_interval_units!(
                &a,
                &a2,
                ShortInterval_DAYS,
                ShortInterval_HOURS,
                ShortInterval_DAYS_TO_HOURS,
                ShortInterval_MINUTES,
                ShortInterval_DAYS_TO_MINUTES,
                ShortInterval_HOURS_TO_MINUTES,
                ShortInterval_SECONDS,
                ShortInterval_DAYS_TO_SECONDS,
                ShortInterval_HOURS_TO_SECONDS,
                ShortInterval_MINUTES_TO_SECONDS,
                LongInterval_YEARS_TO_MONTHS,
                LongInterval_MONTHS,
                LongInterval_YEARS,
            );

            // Containers over every decodable element type.
            macro_rules! check_vec_elem {
                ($($t:ty),* $(,)?) => {$(
                    check_from(&a, &a2,
                        c1::cast_to_vecN_V::<$t>, c2::cast_to_vecN_FV::<$t>,
                        concat!("array<", stringify!($t), ">"));
                )*};
            }
            check_vec_elem!(
                bool,
                i8,
                i16,
                i32,
                u8,
                u16,
                u32,
                u64,
                F32,
                F64,
                Date,
                Time,
                Timestamp,
                TimestampTz,
                ShortInterval,
                LongInterval,
                Uuid,
                ByteArray,
                crate::GeoPoint,
                crate::SqlDecimal<10, 2>,
            );

            // Containers with concrete element types, nullable and not.
            check_from(
                &a,
                &a2,
                c1::cast_to_vecN_V::<i64>,
                c2::cast_to_vecN_FV::<i64>,
                "bigint array",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_vecN_V::<Option<i64>>,
                c2::cast_to_vecN_FV::<Option<i64>>,
                "nullable bigint array",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_mapN_V::<SqlString, i64>,
                c2::cast_to_mapN_FV::<SqlString, i64>,
                "map<varchar,bigint>",
            );
            check_from(
                &a,
                &a2,
                c1::cast_to_mapN_V::<SqlString, Option<i64>>,
                c2::cast_to_mapN_FV::<SqlString, Option<i64>>,
                "map<varchar,bigint null>",
            );

            // Containers with VARIANT elements; compare via the bridge.
            assert_eq!(
                c1::cast_to_vecN_V::<Variant>(a.clone())
                    .map(|o| o.map(|arr| arr.iter().map(FlatVariant::from).collect::<Vec<_>>()))
                    .map_err(|_| ()),
                c2::cast_to_vecN_FV::<FlatVariant>(a2.clone())
                    .map(|o| o.map(|arr| arr.to_vec()))
                    .map_err(|_| ()),
                "variant array cast diverges for {a:?}"
            );

            // Non-null container forms error on mismatched input.
            assert_eq!(
                c1::cast_to_vec_V::<i64>(a.clone()).map_err(|_| ()),
                c2::cast_to_vec_FV::<i64>(a2.clone()).map_err(|_| ()),
                "non-null array cast diverges for {a:?}"
            );
            assert_eq!(
                c1::cast_to_map_V::<SqlString, i64>(a.clone()).map_err(|_| ()),
                c2::cast_to_map_FV::<SqlString, i64>(a2.clone()).map_err(|_| ()),
                "non-null map cast diverges for {a:?}"
            );

            // VARIANT-to-VARIANT, TYPEOF, TO_JSON, and typed indexing.
            assert_eq!(
                FlatVariant::from(&c1::cast_to_V_VN(Some(a.clone())).unwrap()),
                c2::cast_to_FV_FVN(Some(a2.clone())).unwrap(),
                "variant unwrap diverges for {a:?}"
            );
            assert_eq!(
                crate::variant::typeof_(a.clone()),
                f2::typeof_fv_(a2.clone()),
                "typeof diverges for {a:?}"
            );
            assert_eq!(
                crate::string::to_json_V(a.clone()),
                f2::to_json_FV(a2.clone()),
                "to_json diverges for {a:?}"
            );
            for idx in [-1i32, 0, 1, 2] {
                assert_eq!(
                    crate::variant::indexV__(&a, idx).map(|v| FlatVariant::from(&v)),
                    f2::indexFV__(&a2, idx),
                    "index {idx} diverges for {a:?}"
                );
            }
            assert_eq!(
                crate::variant::indexV__(&a, SqlString::from_ref("a"))
                    .map(|v| FlatVariant::from(&v)),
                f2::indexFV__(&a2, SqlString::from_ref("a")),
                "string index diverges for {a:?}"
            );
        }

        // Null propagation through the N-input forms.
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_VN(None).unwrap()),
            c2::cast_to_FV_FVN(None).unwrap(),
        );
        assert_eq!(c2::cast_to_i64N_FVN(None).unwrap(), None);
        assert_eq!(
            c2::cast_to_sN_FVN(None, -1, false).unwrap(),
            c1::cast_to_sN_VN(None, -1, false).unwrap()
        );
    }

    /// Every to-variant cast in the native grid produces the same document
    /// the enum grid produces, for all four nullability shapes.
    #[test]
    fn to_variant_casts_match_enum_grid() {
        use crate::casts as c1;
        use crate::flat_variant::casts as c2;
        use crate::flat_variant::functions as f2;

        macro_rules! check_to_variant {
            ($name:ident, $v:expr) => {{
                ::paste::paste! {
                    let v = $v;
                    assert_eq!(
                        FlatVariant::from(&c1::[<cast_to_V_ $name>](v.clone()).unwrap()),
                        c2::[<cast_to_FV_ $name>](v.clone()).unwrap(),
                        concat!(stringify!($name), ": V form diverges")
                    );
                    assert_eq!(
                        FlatVariant::from(&c1::[<cast_to_VN_ $name>](v.clone()).unwrap().unwrap()),
                        c2::[<cast_to_FVN_ $name>](v.clone()).unwrap().unwrap(),
                        concat!(stringify!($name), ": VN form diverges")
                    );
                    assert_eq!(
                        FlatVariant::from(&c1::[<cast_to_V_ $name N>](Some(v.clone())).unwrap()),
                        c2::[<cast_to_FV_ $name N>](Some(v.clone())).unwrap(),
                        concat!(stringify!($name), ": V/Some form diverges")
                    );
                    assert_eq!(
                        FlatVariant::from(&c1::[<cast_to_V_ $name N>](None).unwrap()),
                        c2::[<cast_to_FV_ $name N>](None).unwrap(),
                        concat!(stringify!($name), ": V/None form diverges")
                    );
                    assert_eq!(
                        FlatVariant::from(&c1::[<cast_to_VN_ $name N>](Some(v.clone())).unwrap().unwrap()),
                        c2::[<cast_to_FVN_ $name N>](Some(v)).unwrap().unwrap(),
                        concat!(stringify!($name), ": VN/Some form diverges")
                    );
                }
            }};
        }

        check_to_variant!(b, true);
        check_to_variant!(i8, -5i8);
        check_to_variant!(i16, -300i16);
        check_to_variant!(i32, 123_456i32);
        check_to_variant!(i64, -9_000_000_000i64);
        check_to_variant!(u8, 200u8);
        check_to_variant!(u16, 60_000u16);
        check_to_variant!(u32, 4_000_000_000u32);
        check_to_variant!(u64, 18_000_000_000_000_000_000u64);
        check_to_variant!(f, F32::new(1.5));
        check_to_variant!(d, F64::new(-2.25));
        check_to_variant!(s, SqlString::from_ref("hello"));
        check_to_variant!(bytes, ByteArray::new(&[1u8, 2, 3]));
        check_to_variant!(Date, Date::from_days(19_000));
        check_to_variant!(Time, Time::from_nanoseconds(43_200_000_000_000));
        check_to_variant!(
            Timestamp,
            Timestamp::from_microseconds(1_700_000_000_000_000)
        );
        check_to_variant!(
            TimestampTz,
            TimestampTz::from_microseconds(1_700_000_000_000_000)
        );
        check_to_variant!(Uuid, Uuid::from_bytes([9; 16]));
        check_to_variant!(GeoPoint, crate::GeoPoint::new(1.0, -2.0));
        let short = ShortInterval::from_microseconds(90_061_000_000);
        check_to_variant!(ShortInterval_DAYS, short);
        check_to_variant!(ShortInterval_HOURS, short);
        check_to_variant!(ShortInterval_DAYS_TO_HOURS, short);
        check_to_variant!(ShortInterval_MINUTES, short);
        check_to_variant!(ShortInterval_DAYS_TO_MINUTES, short);
        check_to_variant!(ShortInterval_HOURS_TO_MINUTES, short);
        check_to_variant!(ShortInterval_SECONDS, short);
        check_to_variant!(ShortInterval_DAYS_TO_SECONDS, short);
        check_to_variant!(ShortInterval_HOURS_TO_SECONDS, short);
        check_to_variant!(ShortInterval_MINUTES_TO_SECONDS, short);
        let long = LongInterval::from_months(25);
        check_to_variant!(LongInterval_YEARS_TO_MONTHS, long);
        check_to_variant!(LongInterval_MONTHS, long);
        check_to_variant!(LongInterval_YEARS, long);

        let dec = crate::SqlDecimal::<10, 2>::try_from(DynamicDecimal::new(12_345, 2)).unwrap();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_SqlDecimal::<10, 2>(dec).unwrap()),
            c2::cast_to_FV_SqlDecimal::<10, 2>(dec).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_SqlDecimalN::<10, 2>(Some(dec)).unwrap()),
            c2::cast_to_FV_SqlDecimalN::<10, 2>(Some(dec)).unwrap(),
        );

        let arr: crate::Array<i32> = vec![1, 2, 3].into();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_vec(arr.clone()).unwrap()),
            c2::cast_to_FV_vec(arr.clone()).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_VN_vec(arr.clone()).unwrap().unwrap()),
            c2::cast_to_FVN_vec(arr.clone()).unwrap().unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_vecN(Some(arr.clone())).unwrap()),
            c2::cast_to_FV_vecN(Some(arr.clone())).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_vecN::<i32>(None).unwrap()),
            c2::cast_to_FV_vecN::<i32>(None).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_VN_vecN(Some(arr.clone())).unwrap().unwrap()),
            c2::cast_to_FVN_vecN(Some(arr)).unwrap().unwrap(),
        );
        let opt_arr: crate::Array<Option<i32>> = vec![Some(1), None].into();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_vec(opt_arr.clone()).unwrap()),
            c2::cast_to_FV_vec(opt_arr).unwrap(),
        );
        // VARIANT elements encode through EncodeFV for FlatVariant itself.
        let var_arr: crate::Array<Variant> = vec![Variant::BigInt(1), Variant::VariantNull].into();
        let var_arr2: crate::Array<FlatVariant> = var_arr
            .iter()
            .map(FlatVariant::from)
            .collect::<Vec<_>>()
            .into();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_vec(var_arr).unwrap()),
            c2::cast_to_FV_vec(var_arr2).unwrap(),
        );

        let map: crate::Map<SqlString, i32> =
            BTreeMap::from([(SqlString::from_ref("a"), 1), (SqlString::from_ref("b"), 2)]).into();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_map(map.clone()).unwrap()),
            c2::cast_to_FV_map(map.clone()).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_VN_map(map.clone()).unwrap().unwrap()),
            c2::cast_to_FVN_map(map.clone()).unwrap().unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_mapN(Some(map.clone())).unwrap()),
            c2::cast_to_FV_mapN(Some(map.clone())).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_V_mapN::<SqlString, i32>(None).unwrap()),
            c2::cast_to_FV_mapN::<SqlString, i32>(None).unwrap(),
        );
        assert_eq!(
            FlatVariant::from(&c1::cast_to_VN_mapN(Some(map.clone())).unwrap().unwrap()),
            c2::cast_to_FVN_mapN(Some(map)).unwrap().unwrap(),
        );

        assert_eq!(
            FlatVariant::from(&crate::variant::variantnull()),
            f2::variantnull_fv(),
        );
    }

    /// The Option-input (`*_FVN`) wrappers, the non-null string/binary and
    /// container forms, the index/typeof/json helper grid, the literal
    /// `From` constructors, and the metadata boundary helpers all agree
    /// with their enum counterparts.
    #[test]
    fn remaining_grid_matches_enum() {
        use crate::casts as c1;
        use crate::flat_variant::casts as c2;
        use crate::flat_variant::functions as f2;

        macro_rules! check_optional_from {
            ($a:expr, $a2:expr, $($name:ident),* $(,)?) => {::paste::paste! {$(
                assert_eq!(c2::[<cast_to_ $name N_FVN>](None).unwrap(), None);
                assert_eq!(
                    c1::[<cast_to_ $name N_VN>](Some($a.clone())).map_err(|_| ()),
                    c2::[<cast_to_ $name N_FVN>](Some($a2.clone())).map_err(|_| ()),
                    concat!(stringify!($name), " optional cast diverges for {:?}"), $a
                );
            )*}};
        }

        for a in [
            Variant::BigInt(7),
            Variant::String(SqlString::from_ref("2020-01-01")),
            Variant::VariantNull,
            Variant::Boolean(true),
        ] {
            let a2 = FlatVariant::from(&a);
            check_optional_from!(
                &a,
                &a2,
                b,
                i8,
                i16,
                i32,
                i64,
                u8,
                u16,
                u32,
                u64,
                f,
                d,
                Date,
                Time,
                Timestamp,
                TimestampTz,
                Uuid,
                GeoPoint,
                ShortInterval_DAYS,
                ShortInterval_HOURS,
                ShortInterval_DAYS_TO_HOURS,
                ShortInterval_MINUTES,
                ShortInterval_DAYS_TO_MINUTES,
                ShortInterval_HOURS_TO_MINUTES,
                ShortInterval_SECONDS,
                ShortInterval_DAYS_TO_SECONDS,
                ShortInterval_HOURS_TO_SECONDS,
                ShortInterval_MINUTES_TO_SECONDS,
                LongInterval_YEARS_TO_MONTHS,
                LongInterval_MONTHS,
                LongInterval_YEARS,
            );
            assert_eq!(
                c1::cast_to_SqlDecimalN_VN::<10, 2>(Some(a.clone())).map_err(|_| ()),
                c2::cast_to_SqlDecimalN_FVN::<10, 2>(Some(a2.clone())).map_err(|_| ()),
            );
            assert_eq!(
                c1::cast_to_sN_VN(Some(a.clone()), -1, false).map_err(|_| ()),
                c2::cast_to_sN_FVN(Some(a2.clone()), -1, false).map_err(|_| ()),
            );
            assert_eq!(
                c1::cast_to_bytesN_VN(Some(a.clone()), -1, false).map_err(|_| ()),
                c2::cast_to_bytesN_FVN(Some(a2.clone()), -1, false).map_err(|_| ()),
            );
            assert_eq!(
                c1::cast_to_vecN_VN::<i64>(Some(a.clone())).map_err(|_| ()),
                c2::cast_to_vecN_FVN::<i64>(Some(a2.clone())).map_err(|_| ()),
            );
            assert_eq!(
                c1::cast_to_mapN_VN::<SqlString, i64>(Some(a.clone())).map_err(|_| ()),
                c2::cast_to_mapN_FVN::<SqlString, i64>(Some(a2.clone())).map_err(|_| ()),
            );
        }
        assert_eq!(c2::cast_to_SqlDecimalN_FVN::<10, 2>(None).unwrap(), None);
        assert_eq!(c2::cast_to_vecN_FVN::<i64>(None).unwrap(), None);
        assert_eq!(c2::cast_to_mapN_FVN::<SqlString, i64>(None).unwrap(), None);
        assert_eq!(c2::cast_to_bytesN_FVN(None, -1, false).unwrap(), None);

        // Non-null result forms on inputs they accept.
        let s = Variant::String(SqlString::from_ref("abc"));
        let s2 = FlatVariant::from(&s);
        assert_eq!(
            c1::cast_to_s_V(s.clone(), -1, false).unwrap(),
            c2::cast_to_s_FV(s2.clone(), -1, false).unwrap(),
        );
        assert_eq!(
            c1::cast_to_s_VN(Some(s.clone()), -1, false).unwrap(),
            c2::cast_to_s_FVN(Some(s2.clone()), -1, false).unwrap(),
        );
        let bin = Variant::Binary(ByteArray::new(&[1, 2]));
        let bin2 = FlatVariant::from(&bin);
        assert_eq!(
            c1::cast_to_bytes_V(bin.clone(), -1, false).unwrap(),
            c2::cast_to_bytes_FV(bin2.clone(), -1, false).unwrap(),
        );
        assert_eq!(
            c1::cast_to_bytes_VN(Some(bin.clone()), -1, false).unwrap(),
            c2::cast_to_bytes_FVN(Some(bin2.clone()), -1, false).unwrap(),
        );
        let arr = Variant::Array(vec![Variant::BigInt(1), Variant::BigInt(2)].into());
        let arr2 = FlatVariant::from(&arr);
        assert_eq!(
            c1::cast_to_vec_VN::<i64>(Some(arr.clone())).unwrap(),
            c2::cast_to_vec_FVN::<i64>(Some(arr2.clone())).unwrap(),
        );
        assert_eq!(c2::cast_to_vec_FVN::<i64>(None).unwrap(), None);
        let map = Variant::Map(
            BTreeMap::from([(
                Variant::String(SqlString::from_ref("k")),
                Variant::BigInt(3),
            )])
            .into(),
        );
        let map2 = FlatVariant::from(&map);
        assert_eq!(
            c1::cast_to_map_VN::<SqlString, i64>(Some(map.clone())).unwrap(),
            c2::cast_to_map_FVN::<SqlString, i64>(Some(map2.clone())).unwrap(),
        );
        assert_eq!(c2::cast_to_map_FVN::<SqlString, i64>(None).unwrap(), None);

        // To-variant decimal VN forms.
        let dec = crate::SqlDecimal::<10, 2>::try_from(DynamicDecimal::new(777, 1)).unwrap();
        assert_eq!(
            FlatVariant::from(&c1::cast_to_VN_SqlDecimal::<10, 2>(dec).unwrap().unwrap()),
            c2::cast_to_FVN_SqlDecimal::<10, 2>(dec).unwrap().unwrap(),
        );
        assert_eq!(
            FlatVariant::from(
                &c1::cast_to_VN_SqlDecimalN::<10, 2>(Some(dec))
                    .unwrap()
                    .unwrap()
            ),
            c2::cast_to_FVN_SqlDecimalN::<10, 2>(Some(dec))
                .unwrap()
                .unwrap(),
        );

        // Index forms over an array and a map, all nullability shapes.
        let expected = crate::variant::indexV_N(&arr, Some(1i32)).map(|v| FlatVariant::from(&v));
        assert_eq!(expected, f2::indexFV_N(&arr2, Some(1i32)));
        assert_eq!(f2::indexFV_N::<i32>(&arr2, None), None);
        assert_eq!(
            crate::variant::indexVN_(&Some(map.clone()), SqlString::from_ref("k"))
                .map(|v| FlatVariant::from(&v)),
            f2::indexFVN_(&Some(map2.clone()), SqlString::from_ref("k")),
        );
        assert_eq!(f2::indexFVN_(&None, SqlString::from_ref("k")), None);
        assert_eq!(
            crate::variant::indexVNN(&Some(map.clone()), Some(SqlString::from_ref("k")))
                .map(|v| FlatVariant::from(&v)),
            f2::indexFVNN(&Some(map2.clone()), Some(SqlString::from_ref("k"))),
        );
        assert_eq!(f2::indexFVNN::<i32>(&None, None), None);

        // TYPEOF, PARSE_JSON, TO_JSON helper forms.
        assert_eq!(
            crate::variant::typeofN(Some(map.clone())),
            f2::typeof_fvN(Some(map2.clone()))
        );
        assert_eq!(crate::variant::typeofN(None), f2::typeof_fvN(None));
        assert_eq!(
            crate::string::parse_json_sN(Some(SqlString::from_ref("[1]")))
                .map(|v| FlatVariant::from(&v)),
            f2::parse_json_fv_sN(Some(SqlString::from_ref("[1]"))),
        );
        assert_eq!(f2::parse_json_fv_sN(None), None);
        assert_eq!(f2::parse_json_fv_nullN(None), None);
        assert_eq!(
            crate::string::to_json_VN(Some(map.clone())),
            f2::to_json_FVN(Some(map2.clone()))
        );
        assert_eq!(f2::to_json_FVN(None), None);

        // Metadata boundary helpers.
        assert_eq!(variant_to_fv(map.clone()), map2);
        assert_eq!(variant_to_fvN(Some(map.clone())), Some(map2.clone()));
        assert_eq!(variant_to_fvN(None), None);

        // Literal From constructors match encoding through the enum.
        macro_rules! check_literal_from {
            ($($v:expr),* $(,)?) => {$(
                let value = $v;
                assert_eq!(
                    FlatVariant::from(value.clone()),
                    FlatVariant::from(&Variant::from(value.clone())),
                    "literal From diverges for {:?}", Variant::from(value.clone())
                );
                assert_eq!(
                    FlatVariant::from(Some(value.clone())),
                    FlatVariant::from(&Variant::from(Some(value))),
                );
            )*};
        }
        check_literal_from!(
            true,
            -5i8,
            -300i16,
            123_456i32,
            -9_000_000_000i64,
            200u8,
            60_000u16,
            4_000_000_000u32,
            18_000_000_000_000_000_000u64,
            F32::new(1.5),
            F64::new(-2.25),
            SqlString::from_ref("lit"),
            Date::from_days(19_000),
            Time::from_nanoseconds(1_000_000_000),
            Timestamp::from_microseconds(1_700_000_000_000_000),
            TimestampTz::from_microseconds(1_700_000_000_000_000),
            ShortInterval::from_microseconds(90_061_000_000),
            LongInterval::from_months(25),
            crate::GeoPoint::new(1.0, -2.0),
            ByteArray::new(&[4u8, 5]),
            Uuid::from_bytes([3; 16]),
            dec,
        );
        let native_arr: crate::Array<i32> = vec![1, 2].into();
        assert_eq!(
            FlatVariant::from(native_arr.clone()),
            FlatVariant::from(&<Variant as From<crate::Array<i32>>>::from(
                native_arr.clone()
            )),
        );
        assert_eq!(
            FlatVariant::from(Some(native_arr.clone())),
            FlatVariant::from(&<Variant as From<Option<crate::Array<i32>>>>::from(Some(
                native_arr
            ))),
        );
        let native_map: crate::Map<SqlString, i32> =
            BTreeMap::from([(SqlString::from_ref("m"), 9)]).into();
        assert_eq!(
            FlatVariant::from(native_map.clone()),
            FlatVariant::from(&<Variant as From<crate::Map<SqlString, i32>>>::from(
                native_map.clone()
            )),
        );
        assert_eq!(
            FlatVariant::from(Some(native_map.clone())),
            FlatVariant::from(
                &<Variant as From<Option<crate::Map<SqlString, i32>>>>::from(Some(native_map))
            ),
        );

        // SqlDecimal as a container element.
        let dec_arr = Variant::Array(vec![Variant::SqlDecimal((123, 2))].into());
        let dec_arr2 = FlatVariant::from(&dec_arr);
        assert_eq!(
            c1::cast_to_vecN_V::<crate::SqlDecimal<10, 2>>(dec_arr).map_err(|_| ()),
            c2::cast_to_vecN_FV::<crate::SqlDecimal<10, 2>>(dec_arr2).map_err(|_| ()),
        );
    }

    /// Float payloads with distinct bit patterns compare and hash like the
    /// enum: negative zero equals zero, NaN equals NaN.
    #[test]
    fn float_edge_semantics_match_enum() {
        let cases = [
            (
                Variant::Double(F64::new(0.0)),
                Variant::Double(F64::new(-0.0)),
            ),
            (
                Variant::Double(F64::new(f64::NAN)),
                Variant::Double(F64::new(f64::from_bits(0x7ff8_0000_0000_0001))),
            ),
            (Variant::Real(F32::new(0.0)), Variant::Real(F32::new(-0.0))),
            (
                Variant::Real(F32::new(f32::NAN)),
                Variant::Real(F32::new(f32::from_bits(0x7fc0_0001))),
            ),
        ];
        for (a, b) in cases {
            let (a2, b2) = (FlatVariant::from(&a), FlatVariant::from(&b));
            assert_eq!(a == b, a2 == b2, "eq diverges for {a:?} vs {b:?}");
            assert_eq!(a.cmp(&b), a2.cmp(&b2), "ord diverges for {a:?} vs {b:?}");
            if a2 == b2 {
                assert_eq!(hash_of(&a2), hash_of(&b2), "hash diverges for {a:?}");
            }
        }
    }

    /// Deserializing through serde_json::Value (which emits plain
    /// f64/u64/i64 numbers instead of serde_json's private decimal
    /// representation) matches the enum, covering the visitor paths that
    /// non-JSON deserializers take.
    #[test]
    fn value_deserializer_matches_enum() {
        let cases = [
            serde_json::json!(1.5),
            serde_json::json!(-7),
            serde_json::json!(18446744073709551615u64),
            serde_json::json!(null),
            serde_json::json!({"a": [1.25, "s", null], "b": true}),
        ];
        for case in cases {
            let v1: Variant = serde_json::from_value(case.clone()).unwrap();
            let v2: FlatVariant = serde_json::from_value(case.clone()).unwrap();
            assert_eq!(
                FlatVariant::from(&v1),
                v2,
                "value parse diverges for {case}"
            );
        }
    }

    /// PARSE_JSON agrees with the enum on valid and invalid input.
    #[test]
    fn parse_json_matches_enum() {
        let cases = [
            r#"null"#,
            r#"true"#,
            r#"-5"#,
            r#"18446744073709551616"#,
            r#"0.1"#,
            r#""hello""#,
            r#"[1, "two", null, {"a": false}]"#,
            r#"{"b": 1, "a": [2.5]}"#,
            // Invalid inputs; both grids return SQL NULL.
            "",
            "{",
            "nul",
            "[1,",
            "\"unterminated",
            "not json at all",
        ];
        for case in cases {
            let v1 = crate::string::parse_json_s(SqlString::from_ref(case));
            let v2 = crate::flat_variant::functions::parse_json_fv_s(SqlString::from_ref(case));
            assert_eq!(
                FlatVariant::from(&v1),
                v2,
                "parse_json diverges for {case:?}"
            );
        }
    }

    /// Both types render identical JSON under every serde config whose
    /// variant format is `Json`, including flavors with non-default scalar
    /// sub-formats (Postgres: decimals as strings, binary as PgHex).
    #[test]
    fn serialize_context_matches_enum() {
        use feldera_types::format::json::JsonFlavor;
        use feldera_types::serde_with_context::SerializationContext;

        let value = Variant::Map(
            [
                (
                    Variant::String(SqlString::from_ref("dec")),
                    Variant::SqlDecimal((12345, 2)),
                ),
                (
                    Variant::String(SqlString::from_ref("bin")),
                    Variant::Binary(ByteArray::new(&[1, 2, 0xab])),
                ),
                (
                    Variant::String(SqlString::from_ref("ts")),
                    Variant::Timestamp(Timestamp::from_microseconds(1_700_000_000_000_000)),
                ),
                (
                    Variant::String(SqlString::from_ref("id")),
                    Variant::Uuid(Uuid::from_bytes([7; 16])),
                ),
                (
                    Variant::String(SqlString::from_ref("arr")),
                    Variant::Array(
                        vec![Variant::SqlDecimal((5, 1)), Variant::Real(1.5.into())].into(),
                    ),
                ),
            ]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into(),
        );
        let value2 = FlatVariant::from(&value);
        for flavor in [JsonFlavor::Default, JsonFlavor::Postgres] {
            let config = SqlSerdeConfig::from(flavor.clone());
            let enum_json = serde_json::to_string(&SerializationContext::new(&config, &value))
                .expect("enum serializes");
            let flat_json = serde_json::to_string(&SerializationContext::new(&config, &value2))
                .expect("flat serializes");
            assert_eq!(enum_json, flat_json, "output diverges under {flavor:?}");
        }
    }

    /// Real JSON parses identically through both types and emits identical
    /// output text; both store map keys sorted, so unsorted and duplicate
    /// input keys normalize the same way.
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
