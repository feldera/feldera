//! The `FV` runtime function grid for [`FlatVariant`]: casts, indexing, and the
//! JSON functions the SQL compiler emits when the flat_variant mode is on.
//!
//! Everything here runs natively on the flat encoding; no enum `Variant` is
//! built on any path. Reads go through `FVRef`, a zero-allocation borrowed
//! view whose match arms mirror the enum implementations in `variant.rs` and
//! `casts.rs` one to one, so conversion semantics (string fallbacks, numeric
//! coercion, error text) stay aligned by construction; writes go through
//! [`EncodeFV`], which appends `[tag][payload]` bytes directly. The in-crate
//! differential tests and the compiler's variant test suites pin the parity.

use std::error::Error;
use std::ops::Range;

use dbsp::algebra::{F32, F64};
use feldera_fxp::DynamicDecimal;

use crate::casts::*;
use crate::error::{SqlResult, SqlRuntimeError};
use crate::flat_variant::{
    Container, FlatVariant, TAG_ARRAY, TAG_BIGINT, TAG_BINARY, TAG_BOOLEAN, TAG_DATE, TAG_DECIMAL,
    TAG_DOUBLE, TAG_GEOMETRY, TAG_INT, TAG_LONG_INTERVAL, TAG_MAP, TAG_REAL, TAG_SHORT_INTERVAL,
    TAG_SMALLINT, TAG_SQL_NULL, TAG_STRING, TAG_TIME, TAG_TIMESTAMP, TAG_TIMESTAMP_TZ, TAG_TINYINT,
    TAG_UBIGINT, TAG_UINT, TAG_USMALLINT, TAG_UTINYINT, TAG_UUID, TAG_VARIANT_NULL, Writer,
    sort_map_entries,
};
use crate::{
    Array, ByteArray, Date, GeoPoint, LongInterval, Map, ShortInterval, SqlDecimal, SqlString,
    Time, Timestamp, TimestampTz, Uuid, to_hex_,
};

/// `Ok(t)` to `Ok(Some(t))`, like `r2o` in casts.rs.
fn r2o<T, E>(result: Result<T, E>) -> Result<Option<T>, E> {
    result.map(Some)
}

// Borrowed view over one encoded value

/// A zero-allocation view of one encoded value. Scalar payloads are decoded
/// to native values; strings and binaries borrow the buffer; containers keep
/// their raw bytes for elementwise recursion.
pub(crate) enum FVRef<'a> {
    SqlNull,
    VariantNull,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Real(F32),
    Double(F64),
    /// (significand, scale)
    Decimal(i128, u8),
    String(&'a str),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    TimestampTz(TimestampTz),
    ShortInterval(ShortInterval),
    LongInterval(LongInterval),
    Binary(&'a [u8]),
    Geometry(GeoPoint),
    Uuid(Uuid),
    /// Container arms carry no payload; container casts recurse over the
    /// original bytes through [`Container`] directly.
    Array,
    Map,
}

pub(crate) fn view(bytes: &[u8]) -> FVRef<'_> {
    let p = &bytes[1..];
    match bytes[0] {
        TAG_SQL_NULL => FVRef::SqlNull,
        TAG_VARIANT_NULL => FVRef::VariantNull,
        TAG_BOOLEAN => FVRef::Boolean(p[0] != 0),
        TAG_TINYINT => FVRef::TinyInt(p[0] as i8),
        TAG_SMALLINT => FVRef::SmallInt(i16::from_le_bytes(p.try_into().unwrap())),
        TAG_INT => FVRef::Int(i32::from_le_bytes(p.try_into().unwrap())),
        TAG_BIGINT => FVRef::BigInt(i64::from_le_bytes(p.try_into().unwrap())),
        TAG_UTINYINT => FVRef::UTinyInt(p[0]),
        TAG_USMALLINT => FVRef::USmallInt(u16::from_le_bytes(p.try_into().unwrap())),
        TAG_UINT => FVRef::UInt(u32::from_le_bytes(p.try_into().unwrap())),
        TAG_UBIGINT => FVRef::UBigInt(u64::from_le_bytes(p.try_into().unwrap())),
        TAG_REAL => FVRef::Real(F32::new(f32::from_le_bytes(p.try_into().unwrap()))),
        TAG_DOUBLE => FVRef::Double(F64::new(f64::from_le_bytes(p.try_into().unwrap()))),
        TAG_DECIMAL => FVRef::Decimal(i128::from_le_bytes(p[..16].try_into().unwrap()), p[16]),
        TAG_STRING => FVRef::String(std::str::from_utf8(p).expect("encoded string is UTF-8")),
        TAG_DATE => FVRef::Date(Date::from_days(i32::from_le_bytes(p.try_into().unwrap()))),
        TAG_TIME => FVRef::Time(Time::from_nanoseconds(u64::from_le_bytes(
            p.try_into().unwrap(),
        ))),
        TAG_TIMESTAMP => FVRef::Timestamp(Timestamp::from_microseconds(i64::from_le_bytes(
            p.try_into().unwrap(),
        ))),
        TAG_TIMESTAMP_TZ => FVRef::TimestampTz(TimestampTz::from_microseconds(i64::from_le_bytes(
            p.try_into().unwrap(),
        ))),
        TAG_SHORT_INTERVAL => FVRef::ShortInterval(ShortInterval::from_microseconds(
            i64::from_le_bytes(p.try_into().unwrap()),
        )),
        TAG_LONG_INTERVAL => FVRef::LongInterval(LongInterval::from_months(i32::from_le_bytes(
            p.try_into().unwrap(),
        ))),
        TAG_BINARY => FVRef::Binary(p),
        TAG_GEOMETRY => FVRef::Geometry(GeoPoint::new(
            f64::from_le_bytes(p[..8].try_into().unwrap()),
            f64::from_le_bytes(p[8..].try_into().unwrap()),
        )),
        TAG_UUID => FVRef::Uuid(Uuid::from_bytes(p.try_into().unwrap())),
        TAG_ARRAY => FVRef::Array,
        TAG_MAP => FVRef::Map,
        tag => unreachable!("invalid tag {tag}"),
    }
}

/// The SQL type name of an encoded value; mirrors
/// `Variant::get_type_string`.
pub(crate) fn type_string(bytes: &[u8]) -> &'static str {
    match bytes[0] {
        TAG_SQL_NULL => "NULL",
        TAG_VARIANT_NULL => "VARIANT",
        TAG_BOOLEAN => "BOOLEAN",
        TAG_TINYINT => "TINYINT",
        TAG_SMALLINT => "SMALLINT",
        TAG_INT => "INTEGER",
        TAG_BIGINT => "BIGINT",
        TAG_UTINYINT => "TINYINT UNSIGNED",
        TAG_USMALLINT => "SMALLINT UNSIGNED",
        TAG_UINT => "INTEGER UNSIGNED",
        TAG_UBIGINT => "BIGINT UNSIGNED",
        TAG_REAL => "REAL",
        TAG_DOUBLE => "DOUBLE",
        TAG_DECIMAL => "DECIMAL",
        TAG_STRING => "VARCHAR",
        TAG_DATE => "DATE",
        TAG_TIME => "TIME",
        TAG_TIMESTAMP => "TIMESTAMP",
        TAG_TIMESTAMP_TZ => "TIMESTAMP WITH TIME ZONE",
        TAG_SHORT_INTERVAL => "SHORTINTERVAL",
        TAG_LONG_INTERVAL => "LONGINTERVAL",
        TAG_GEOMETRY => "GEOPOINT",
        TAG_BINARY => "BINARY",
        TAG_ARRAY => "ARRAY",
        TAG_MAP => "MAP",
        TAG_UUID => "UUID",
        tag => unreachable!("invalid tag {tag}"),
    }
}

fn cannot_convert(bytes: &[u8], to: &str) -> Box<SqlRuntimeError> {
    SqlRuntimeError::from_string(format!(
        "variant is {}, which cannot be converted to {}",
        type_string(bytes),
        to,
    ))
}

// Encoding: native value -> flat bytes

/// Appends one complete encoded value to the writer. The counterpart of the
/// enum's `From<T> for Variant`.
///
/// Public because it bounds the public container-cast functions; not part of
/// the supported API.
#[doc(hidden)]
pub trait EncodeFV {
    fn encode(&self, w: &mut Writer) -> Range<usize>;
}

/// Build a right-sized document from one encodable value.
pub(crate) fn encode_document<T: EncodeFV + ?Sized>(value: &T) -> FlatVariant {
    let mut w = Writer {
        out: Vec::with_capacity(64),
    };
    let range = value.encode(&mut w);
    FlatVariant::from_bytes(&w.out[range])
}

macro_rules! encode_le {
    ($($t:ty => $tag:expr),* $(,)?) => {$(
        impl EncodeFV for $t {
            #[inline]
            fn encode(&self, w: &mut Writer) -> Range<usize> {
                w.scalar($tag, &self.to_le_bytes())
            }
        }
    )*};
}

encode_le!(
    i8 => TAG_TINYINT,
    i16 => TAG_SMALLINT,
    i32 => TAG_INT,
    i64 => TAG_BIGINT,
    u8 => TAG_UTINYINT,
    u16 => TAG_USMALLINT,
    u32 => TAG_UINT,
    u64 => TAG_UBIGINT,
);

impl EncodeFV for bool {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_BOOLEAN, &[*self as u8])
    }
}

impl EncodeFV for F32 {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_REAL, &self.into_inner().to_le_bytes())
    }
}

impl EncodeFV for F64 {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_DOUBLE, &self.into_inner().to_le_bytes())
    }
}

pub(crate) fn decimal_payload(significand: i128, scale: u8) -> [u8; 17] {
    let mut payload = [0u8; 17];
    payload[..16].copy_from_slice(&significand.to_le_bytes());
    payload[16] = scale;
    payload
}

impl<const P: usize, const S: usize> EncodeFV for SqlDecimal<P, S> {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        // Mirrors From<SqlDecimal<P, S>> for Variant: through DynamicDecimal.
        let dd = DynamicDecimal::from(*self);
        w.scalar(
            TAG_DECIMAL,
            &decimal_payload(dd.significand(), dd.exponent()),
        )
    }
}

impl EncodeFV for SqlString {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_STRING, self.str().as_bytes())
    }
}

impl EncodeFV for ByteArray {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_BINARY, self.as_slice())
    }
}

impl EncodeFV for Date {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_DATE, &self.days().to_le_bytes())
    }
}

impl EncodeFV for Time {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_TIME, &self.nanoseconds().to_le_bytes())
    }
}

impl EncodeFV for Timestamp {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_TIMESTAMP, &self.microseconds().to_le_bytes())
    }
}

impl EncodeFV for TimestampTz {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_TIMESTAMP_TZ, &self.microseconds().to_le_bytes())
    }
}

impl EncodeFV for ShortInterval {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_SHORT_INTERVAL, &self.microseconds().to_le_bytes())
    }
}

impl EncodeFV for LongInterval {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_LONG_INTERVAL, &self.months().to_le_bytes())
    }
}

impl EncodeFV for GeoPoint {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        let mut payload = [0u8; 16];
        payload[..8].copy_from_slice(&self.left().into_inner().to_le_bytes());
        payload[8..].copy_from_slice(&self.right().into_inner().to_le_bytes());
        w.scalar(TAG_GEOMETRY, &payload)
    }
}

impl EncodeFV for Uuid {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.scalar(TAG_UUID, &self.to_bytes()[..])
    }
}

impl EncodeFV for FlatVariant {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        w.raw(self.as_bytes())
    }
}

/// None encodes as SqlNull, mirroring `From<Option<T>> for Variant`.
impl<T: EncodeFV> EncodeFV for Option<T> {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        match self {
            None => w.scalar(TAG_SQL_NULL, &[]),
            Some(value) => value.encode(w),
        }
    }
}

impl<T: EncodeFV> EncodeFV for Array<T> {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        let children: Vec<Range<usize>> = self.iter().map(|item| item.encode(w)).collect();
        w.array(&children)
    }
}

impl<K: EncodeFV, V: EncodeFV> EncodeFV for Map<K, V> {
    fn encode(&self, w: &mut Writer) -> Range<usize> {
        let mut entries: Vec<(Range<usize>, Range<usize>)> = self
            .iter()
            .map(|(k, v)| (k.encode(w), v.encode(w)))
            .collect();
        // Key encodings are injective, so entries from a well-formed map
        // never collapse; sorting matches the canonical key order.
        sort_map_entries(&w.out, &mut entries);
        w.map(&entries)
    }
}

// Decoding: flat bytes -> native value (container element semantics)

/// Converts one encoded value to a native value, mirroring the enum's
/// `TryFrom<Variant>` impls arm for arm (`into!`, `into_numeric!`, and the
/// manual SqlString/SqlDecimal/Array/Map impls in variant.rs).
///
/// Public because it bounds the public container-cast functions; not part of
/// the supported API.
#[doc(hidden)]
pub trait DecodeFV: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>>;

    /// Option semantics: nulls become None for concrete targets. `FlatVariant`
    /// overrides this to always wrap, matching the enum, where
    /// `Option<Variant>` gets its conversion from core's `From<T> for
    /// Option<T>` and a JSON null stays a variant-null value.
    fn decode_option(bytes: &[u8]) -> Result<Option<Self>, Box<dyn Error>> {
        match bytes[0] {
            TAG_SQL_NULL | TAG_VARIANT_NULL => Ok(None),
            _ => Ok(Some(Self::decode(bytes)?)),
        }
    }
}

macro_rules! decode_exact {
    // into!: exact tag, string fallback, else error.
    ($type:ty, $pattern:pat => $value:expr, $cast_s:ident, $sqlname:expr) => {
        impl DecodeFV for $type {
            fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
                match view(bytes) {
                    FVRef::String(x) => Ok($cast_s(SqlString::from_ref(x))?),
                    $pattern => Ok($value),
                    _ => Err(cannot_convert(bytes, $sqlname).into()),
                }
            }
        }
    };
    // into_no_string!: exact tag only.
    ($type:ty, $pattern:pat => $value:expr, $sqlname:expr) => {
        impl DecodeFV for $type {
            fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
                match view(bytes) {
                    $pattern => Ok($value),
                    _ => Err(cannot_convert(bytes, $sqlname).into()),
                }
            }
        }
    };
}

decode_exact!(bool, FVRef::Boolean(x) => x, cast_to_b_s, "BOOLEAN");
decode_exact!(Date, FVRef::Date(x) => x, cast_to_Date_s, "DATE");
decode_exact!(Time, FVRef::Time(x) => x, cast_to_Time_s, "TIME");
decode_exact!(Timestamp, FVRef::Timestamp(x) => x, cast_to_Timestamp_s, "TIMESTAMP");
decode_exact!(TimestampTz, FVRef::TimestampTz(x) => x, cast_to_TimestampTz_s, "TIMESTAMP WITH TIME ZONE");
decode_exact!(ShortInterval, FVRef::ShortInterval(x) => x, cast_to_ShortInterval_DAYS_TO_MINUTES_s, "INTERVAL DAYS TO MINUTES");
decode_exact!(LongInterval, FVRef::LongInterval(x) => x, cast_to_LongInterval_YEARS_TO_MONTHS_s, "INTERVAL YEARS TO MONTHS");
decode_exact!(Uuid, FVRef::Uuid(x) => x, cast_to_Uuid_s, "UUID");
decode_exact!(GeoPoint, FVRef::Geometry(x) => x, "GEOPOINT");
decode_exact!(ByteArray, FVRef::Binary(x) => ByteArray::new(x), "BINARY");

macro_rules! decode_numeric {
    ($type:ty, $name:ident, $sqlname:expr) => {
        ::paste::paste! {
            impl DecodeFV for $type {
                fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
                    match view(bytes) {
                        FVRef::String(x) => Ok([<cast_to_ $name _s>](SqlString::from_ref(x))?),
                        FVRef::TinyInt(x) => Ok([<cast_to_ $name _i8>](x)?),
                        FVRef::SmallInt(x) => Ok([<cast_to_ $name _i16>](x)?),
                        FVRef::Int(x) => Ok([<cast_to_ $name _i32>](x)?),
                        FVRef::BigInt(x) => Ok([<cast_to_ $name _i64>](x)?),
                        FVRef::UTinyInt(x) => Ok([<cast_to_ $name _u8>](x)?),
                        FVRef::USmallInt(x) => Ok([<cast_to_ $name _u16>](x)?),
                        FVRef::UInt(x) => Ok([<cast_to_ $name _u32>](x)?),
                        FVRef::UBigInt(x) => Ok([<cast_to_ $name _u64>](x)?),
                        FVRef::Real(x) => Ok([<cast_to_ $name _f>](x)?),
                        FVRef::Double(x) => Ok([<cast_to_ $name _d>](x)?),
                        FVRef::Decimal(sig, scale) => {
                            match i128::try_from(DynamicDecimal::new(sig, scale)) {
                                Ok(value) => Ok([<cast_to_ $name _i128>](value)?),
                                Err(_) => Err(cannot_convert(bytes, $sqlname).into()),
                            }
                        }
                        _ => Err(cannot_convert(bytes, $sqlname).into()),
                    }
                }
            }
        }
    };
}

decode_numeric!(i8, i8, "TINYINT");
decode_numeric!(i16, i16, "SMALLINT");
decode_numeric!(i32, i32, "INTEGER");
decode_numeric!(i64, i64, "BIGINT");
decode_numeric!(u8, u8, "TINYINT UNSIGNED");
decode_numeric!(u16, u16, "SMALLINT UNSIGNED");
decode_numeric!(u32, u32, "INTEGER UNSIGNED");
decode_numeric!(u64, u64, "BIGINT UNSIGNED");
decode_numeric!(F32, f, "REAL");
decode_numeric!(F64, d, "DOUBLE");

/// Mirrors `TryFrom<Variant> for SqlString` (variant.rs): renders scalars.
impl DecodeFV for SqlString {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        Ok(match view(bytes) {
            FVRef::Boolean(x) => SqlString::from_ref(if x { "true" } else { "false" }),
            FVRef::TinyInt(x) => SqlString::from(format!("{x}")),
            FVRef::SmallInt(x) => SqlString::from(format!("{x}")),
            FVRef::Int(x) => SqlString::from(format!("{x}")),
            FVRef::BigInt(x) => SqlString::from(format!("{x}")),
            FVRef::UTinyInt(x) => SqlString::from(format!("{x}")),
            FVRef::USmallInt(x) => SqlString::from(format!("{x}")),
            FVRef::UInt(x) => SqlString::from(format!("{x}")),
            FVRef::UBigInt(x) => SqlString::from(format!("{x}")),
            FVRef::Decimal(sig, scale) => {
                SqlString::from(format!("{}", DynamicDecimal::new(sig, scale)))
            }
            FVRef::Real(x) => {
                let mut buffer = ryu::Buffer::new();
                SqlString::from_ref(buffer.format(x.into_inner()))
            }
            FVRef::Double(x) => {
                let mut buffer = ryu::Buffer::new();
                SqlString::from_ref(buffer.format(x.into_inner()))
            }
            FVRef::String(x) => SqlString::from_ref(x),
            FVRef::Date(x) => SqlString::from(x.to_string()),
            FVRef::Time(x) => SqlString::from(x.to_string()),
            FVRef::Timestamp(x) => SqlString::from(x.to_string()),
            FVRef::TimestampTz(x) => SqlString::from(x.to_string()),
            FVRef::ShortInterval(x) => SqlString::from(x.to_string()),
            FVRef::LongInterval(x) => SqlString::from(x.to_string()),
            FVRef::Binary(x) => to_hex_(ByteArray::new(x)),
            FVRef::Uuid(x) => SqlString::from(format!("{x}")),
            // GeoPoint, Map, and Array have no cast to string.
            _ => return Err(cannot_convert(bytes, "CHAR").into()),
        })
    }
}

/// Mirrors `TryFrom<Variant> for SqlDecimal<P, S>` (variant.rs:850-899).
impl<const P: usize, const S: usize> DecodeFV for SqlDecimal<P, S> {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        match cast_to_SqlDecimalN_FV::<P, S>(FlatVariant::from_bytes(bytes))? {
            Some(value) => Ok(value),
            None => Err(cannot_convert(bytes, "DECIMAL").into()),
        }
    }
}

impl DecodeFV for FlatVariant {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        Ok(FlatVariant::from_bytes(bytes))
    }

    fn decode_option(bytes: &[u8]) -> Result<Option<Self>, Box<dyn Error>> {
        Ok(Some(FlatVariant::from_bytes(bytes)))
    }
}

impl<T: DecodeFV> DecodeFV for Option<T> {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        T::decode_option(bytes)
    }
}

impl<T: DecodeFV> DecodeFV for Array<T> {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        match bytes[0] {
            TAG_ARRAY => {
                let c = Container::new(bytes);
                let mut items = Vec::with_capacity(c.count);
                for i in 0..c.count {
                    items.push(T::decode(&bytes[c.element(i)])?);
                }
                Ok(items.into())
            }
            _ => Err(SqlRuntimeError::from_string("not an array".to_string()).into()),
        }
    }
}

impl<K: DecodeFV + Ord, V: DecodeFV> DecodeFV for Map<K, V> {
    fn decode(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        match bytes[0] {
            TAG_MAP => {
                let c = Container::new(bytes);
                let mut result = std::collections::BTreeMap::new();
                for i in 0..c.count {
                    let k = K::decode(&bytes[c.element(i)])?;
                    let v = V::decode(&bytes[c.map_value(i)])?;
                    result.insert(k, v);
                }
                Ok(result.into())
            }
            _ => Err(SqlRuntimeError::from_string("not a map".to_string()).into()),
        }
    }
}

// Scalar casts (the cast_to_* grid emitted by the compiler)

macro_rules! cast_to_flat_variant {
    ($name: ident $(< $( const $var:ident : $ty: ty),* >)?, $type: ty) => {
        ::paste::paste! {
            // cast_to_FV_i32
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ FV_ $name >] $(< $( const $var : $ty),* >)? ( value: $type ) -> SqlResult<FlatVariant> {
                Ok(encode_document(&value))
            }

            // cast_to_FVN_i32
            #[doc(hidden)]
            pub fn [<cast_to_ FVN_ $name >] $(< $( const $var : $ty),* >)? ( value: $type ) -> SqlResult<Option<FlatVariant>> {
                Ok(Some(encode_document(&value)))
            }

            // cast_to_FV_i32N; None becomes SqlNull.
            #[doc(hidden)]
            pub fn [<cast_to_ FV_ $name N>] $(< $( const $var : $ty),* >)? ( value: Option<$type> ) -> SqlResult<FlatVariant> {
                Ok(encode_document(&value))
            }

            // cast_to_FVN_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ FVN_ $name N>] $(< $( const $var : $ty),* >)? ( value: Option<$type> ) -> SqlResult<Option<FlatVariant>> {
                Ok(Some(encode_document(&value)))
            }
        }
    };
}

/// From-variant casts, mirroring `cast_from_variant!`: exact tag, string
/// fallback, else None.
macro_rules! cast_from_flat_variant {
    ($name: ident, $type: ty, $pattern:pat => $value:expr) => {
        ::paste::paste! {
            // cast_to_i32N_FV
            #[doc(hidden)]
            pub fn [< cast_to_ $name N _FV >](value: FlatVariant) -> SqlResult<Option<$type>> {
                match view(value.as_bytes()) {
                    FVRef::String(x) => r2o([< cast_to_ $name _s>](SqlString::from_ref(x))),
                    $pattern => Ok(Some($value)),
                    _ => Ok(None),
                }
            }

            // cast_to_i32N_FVN
            #[doc(hidden)]
            pub fn [<cast_to_ $name N_ FVN >]( value: Option<FlatVariant> ) -> SqlResult<Option<$type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $name N_FV >](value),
                }
            }
        }
    };
}

/// From-variant numeric casts, mirroring `cast_from_variant_numeric!`.
macro_rules! cast_from_flat_variant_numeric {
    ($name: ident, $type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [< cast_to_ $name N _FV >](value: FlatVariant) -> SqlResult<Option<$type>> {
                match view(value.as_bytes()) {
                    FVRef::String(x) => r2o([< cast_to_ $name _s>](SqlString::from_ref(x))),
                    FVRef::TinyInt(x) => r2o([< cast_to_ $name _i8 >](x)),
                    FVRef::SmallInt(x) => r2o([< cast_to_ $name _i16 >](x)),
                    FVRef::Int(x) => r2o([< cast_to_ $name _i32 >](x)),
                    FVRef::BigInt(x) => r2o([< cast_to_ $name _i64 >](x)),
                    FVRef::UTinyInt(x) => r2o([< cast_to_ $name _u8 >](x)),
                    FVRef::USmallInt(x) => r2o([< cast_to_ $name _u16 >](x)),
                    FVRef::UInt(x) => r2o([< cast_to_ $name _u32 >](x)),
                    FVRef::UBigInt(x) => r2o([< cast_to_ $name _u64 >](x)),
                    FVRef::Real(x) => r2o([< cast_to_ $name _f >](x)),
                    FVRef::Double(x) => r2o([< cast_to_ $name _d >](x)),
                    FVRef::Decimal(sig, scale) => {
                        let dd = DynamicDecimal::new(sig, scale);
                        match <$type>::try_from(dd) {
                            Ok(value) => Ok(Some(value)),
                            Err(e) => Err(SqlRuntimeError::from_string(format!(
                                "Error converting '{sig}' to {}: {}",
                                crate::tn!($name), e
                            ))),
                        }
                    },
                    _ => Ok(None),
                }
            }

            #[doc(hidden)]
            pub fn [<cast_to_ $name N_ FVN >]( value: Option<FlatVariant> ) -> SqlResult<Option<$type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $name N_FV >](value),
                }
            }
        }
    };
}

cast_to_flat_variant!(b, bool);
cast_from_flat_variant!(b, bool, FVRef::Boolean(x) => x);
cast_to_flat_variant!(i8, i8);
cast_from_flat_variant_numeric!(i8, i8);
cast_to_flat_variant!(i16, i16);
cast_from_flat_variant_numeric!(i16, i16);
cast_to_flat_variant!(i32, i32);
cast_from_flat_variant_numeric!(i32, i32);
cast_to_flat_variant!(i64, i64);
cast_from_flat_variant_numeric!(i64, i64);
cast_to_flat_variant!(u8, u8);
cast_from_flat_variant_numeric!(u8, u8);
cast_to_flat_variant!(u16, u16);
cast_from_flat_variant_numeric!(u16, u16);
cast_to_flat_variant!(u32, u32);
cast_from_flat_variant_numeric!(u32, u32);
cast_to_flat_variant!(u64, u64);
cast_from_flat_variant_numeric!(u64, u64);
cast_to_flat_variant!(f, F32);
cast_from_flat_variant_numeric!(f, F32);
cast_to_flat_variant!(d, F64);
cast_from_flat_variant_numeric!(d, F64);
cast_to_flat_variant!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>);
cast_to_flat_variant!(s, SqlString);
cast_to_flat_variant!(bytes, ByteArray);
cast_to_flat_variant!(Date, Date);
cast_from_flat_variant!(Date, Date, FVRef::Date(x) => x);
cast_to_flat_variant!(Time, Time);
cast_from_flat_variant!(Time, Time, FVRef::Time(x) => x);
cast_to_flat_variant!(Uuid, Uuid);
cast_from_flat_variant!(Uuid, Uuid, FVRef::Uuid(x) => x);
cast_to_flat_variant!(Timestamp, Timestamp);
cast_from_flat_variant!(Timestamp, Timestamp, FVRef::Timestamp(x) => x);
cast_to_flat_variant!(TimestampTz, TimestampTz);
cast_from_flat_variant!(TimestampTz, TimestampTz, FVRef::TimestampTz(x) => x);
cast_to_flat_variant!(GeoPoint, GeoPoint);
cast_from_flat_variant!(GeoPoint, GeoPoint, FVRef::Geometry(x) => x);

macro_rules! cast_flat_variant_interval {
    ($name: ident, $type: ty, $refvariant: ident) => {
        cast_to_flat_variant!($name, $type);
        cast_from_flat_variant!($name, $type, FVRef::$refvariant(x) => x);
    };
}

cast_flat_variant_interval!(ShortInterval_DAYS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_HOURS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_DAYS_TO_HOURS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_MINUTES, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_DAYS_TO_MINUTES, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_HOURS_TO_MINUTES, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_SECONDS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_DAYS_TO_SECONDS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(ShortInterval_HOURS_TO_SECONDS, ShortInterval, ShortInterval);
cast_flat_variant_interval!(
    ShortInterval_MINUTES_TO_SECONDS,
    ShortInterval,
    ShortInterval
);
cast_flat_variant_interval!(LongInterval_YEARS_TO_MONTHS, LongInterval, LongInterval);
cast_flat_variant_interval!(LongInterval_MONTHS, LongInterval, LongInterval);
cast_flat_variant_interval!(LongInterval_YEARS, LongInterval, LongInterval);

// String and binary from-variant casts carry size arguments; they mirror
// cast_to_s_V / cast_to_bytes_V in casts.rs.

#[doc(hidden)]
pub fn cast_to_s_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<SqlString> {
    // This function should never be called (the compiler emits the nullable
    // result form), same caveat as cast_to_s_V.
    let result = SqlString::decode(value.as_bytes())
        .map_err(|e| SqlRuntimeError::from_string(e.to_string()))?;
    limit_or_size_string(result.str(), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_FVN(value: Option<FlatVariant>, size: i32, fixed: bool) -> SqlResult<SqlString> {
    // This function should never be called.
    cast_to_s_FV(value.unwrap(), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_sN_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<Option<SqlString>> {
    match SqlString::decode(value.as_bytes()) {
        Err(_) => Ok(None),
        Ok(result) => r2o(limit_or_size_string(result.str(), size, fixed)),
    }
}

#[doc(hidden)]
pub fn cast_to_sN_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<SqlString>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_sN_FV(value, size, fixed),
    }
}

#[doc(hidden)]
pub fn cast_to_bytes_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<ByteArray> {
    match ByteArray::decode(value.as_bytes()) {
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to BINARY: {}",
            e
        ))),
        Ok(result) => Ok(ByteArray::with_size(result.as_slice(), size, fixed)),
    }
}

#[doc(hidden)]
pub fn cast_to_bytes_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<ByteArray> {
    match value {
        None => Err(cast_null("BINARY")),
        Some(value) => cast_to_bytes_FV(value, size, fixed),
    }
}

#[doc(hidden)]
pub fn cast_to_bytesN_FV(
    value: FlatVariant,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<ByteArray>> {
    match ByteArray::decode(value.as_bytes()) {
        Err(_) => Ok(None),
        Ok(value) => Ok(Some(ByteArray::with_size(value.as_slice(), size, fixed))),
    }
}

#[doc(hidden)]
pub fn cast_to_bytesN_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_bytesN_FV(value, size, fixed),
    }
}

/// Mirrors cast_to_SqlDecimalN_V (casts.rs) arm for arm.
#[doc(hidden)]
pub fn cast_to_SqlDecimalN_FV<const P: usize, const S: usize>(
    value: FlatVariant,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match view(value.as_bytes()) {
        FVRef::String(x) => r2o(cast_to_SqlDecimal_s::<P, S>(SqlString::from_ref(x))),
        FVRef::TinyInt(i) => r2o(cast_to_SqlDecimal_i8::<P, S>(i)),
        FVRef::SmallInt(i) => r2o(cast_to_SqlDecimal_i16::<P, S>(i)),
        FVRef::Int(i) => r2o(cast_to_SqlDecimal_i32::<P, S>(i)),
        FVRef::BigInt(i) => r2o(cast_to_SqlDecimal_i64::<P, S>(i)),
        FVRef::UTinyInt(i) => r2o(cast_to_SqlDecimal_u8::<P, S>(i)),
        FVRef::USmallInt(i) => r2o(cast_to_SqlDecimal_u16::<P, S>(i)),
        FVRef::UInt(i) => r2o(cast_to_SqlDecimal_u32::<P, S>(i)),
        FVRef::UBigInt(i) => r2o(cast_to_SqlDecimal_u64::<P, S>(i)),
        FVRef::Real(f) => r2o(cast_to_SqlDecimal_f::<P, S>(f)),
        FVRef::Double(f) => r2o(cast_to_SqlDecimal_d::<P, S>(f)),
        FVRef::Decimal(sig, scale) => {
            let dd = DynamicDecimal::new(sig, scale);
            match SqlDecimal::<P, S>::try_from(dd) {
                Err(e) => Err(SqlRuntimeError::from_string(format!(
                    "Error while converting 'VARIANT({sig}E-{scale})' to DECIMAL({P}, {S}): {}",
                    e
                ))),
                Ok(value) => Ok(Some(value)),
            }
        }
        _ => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_SqlDecimalN_FVN<const P: usize, const S: usize>(
    value: Option<FlatVariant>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_FV::<P, S>(value),
    }
}

// Array and map casts

#[doc(hidden)]
pub fn cast_to_FV_vec<T: EncodeFV>(vec: Array<T>) -> SqlResult<FlatVariant> {
    Ok(encode_document(&vec))
}

#[doc(hidden)]
pub fn cast_to_FVN_vec<T: EncodeFV>(vec: Array<T>) -> SqlResult<Option<FlatVariant>> {
    r2o(cast_to_FV_vec(vec))
}

#[doc(hidden)]
pub fn cast_to_FV_vecN<T: EncodeFV>(vec: Option<Array<T>>) -> SqlResult<FlatVariant> {
    Ok(encode_document(&vec))
}

#[doc(hidden)]
pub fn cast_to_FVN_vecN<T: EncodeFV>(vec: Option<Array<T>>) -> SqlResult<Option<FlatVariant>> {
    r2o(cast_to_FV_vecN(vec))
}

#[doc(hidden)]
pub fn cast_to_vec_FV<T: DecodeFV>(value: FlatVariant) -> SqlResult<Array<T>> {
    match Array::<T>::decode(value.as_bytes()) {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to ARRAY: {}",
            e
        ))),
    }
}

#[doc(hidden)]
pub fn cast_to_vec_FVN<T: DecodeFV>(value: Option<FlatVariant>) -> SqlResult<Option<Array<T>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_vecN_FV(value),
    }
}

#[doc(hidden)]
pub fn cast_to_vecN_FV<T: DecodeFV>(value: FlatVariant) -> SqlResult<Option<Array<T>>> {
    match Array::<T>::decode(value.as_bytes()) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_vecN_FVN<T: DecodeFV>(value: Option<FlatVariant>) -> SqlResult<Option<Array<T>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_vecN_FV(value),
    }
}

#[doc(hidden)]
pub fn cast_to_FV_FVN(value: Option<FlatVariant>) -> SqlResult<FlatVariant> {
    match value {
        None => Ok(FlatVariant::sql_null()),
        Some(x) => Ok(x),
    }
}

#[doc(hidden)]
pub fn cast_to_FV_map<K: EncodeFV, V: EncodeFV>(map: Map<K, V>) -> SqlResult<FlatVariant> {
    Ok(encode_document(&map))
}

#[doc(hidden)]
pub fn cast_to_FVN_map<K: EncodeFV, V: EncodeFV>(map: Map<K, V>) -> SqlResult<Option<FlatVariant>> {
    r2o(cast_to_FV_map(map))
}

#[doc(hidden)]
pub fn cast_to_FV_mapN<K: EncodeFV, V: EncodeFV>(map: Option<Map<K, V>>) -> SqlResult<FlatVariant> {
    Ok(encode_document(&map))
}

#[doc(hidden)]
pub fn cast_to_FVN_mapN<K: EncodeFV, V: EncodeFV>(
    map: Option<Map<K, V>>,
) -> SqlResult<Option<FlatVariant>> {
    r2o(cast_to_FV_mapN(map))
}

#[doc(hidden)]
pub fn cast_to_map_FV<K: DecodeFV + Ord, V: DecodeFV>(value: FlatVariant) -> SqlResult<Map<K, V>> {
    match Map::<K, V>::decode(value.as_bytes()) {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to MAP: {}",
            e
        ))),
    }
}

#[doc(hidden)]
pub fn cast_to_map_FVN<K: DecodeFV + Ord, V: DecodeFV>(
    value: Option<FlatVariant>,
) -> SqlResult<Option<Map<K, V>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_mapN_FV(value),
    }
}

#[doc(hidden)]
pub fn cast_to_mapN_FV<K: DecodeFV + Ord, V: DecodeFV>(
    value: FlatVariant,
) -> SqlResult<Option<Map<K, V>>> {
    match Map::<K, V>::decode(value.as_bytes()) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_mapN_FVN<K: DecodeFV + Ord, V: DecodeFV>(
    value: Option<FlatVariant>,
) -> SqlResult<Option<Map<K, V>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_mapN_FV(value),
    }
}

// Indexing (VARIANT_INDEX opcode), native on the flat encoding

// Return type is always Option<FlatVariant>, matching the indexV grid.
#[doc(hidden)]
pub fn indexFV__<T>(value: &FlatVariant, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    value.index(&index.into())
}

#[doc(hidden)]
pub fn indexFV_N<T>(value: &FlatVariant, index: Option<T>) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    let index = index?;
    indexFV__(value, index)
}

#[doc(hidden)]
pub fn indexFVN_<T>(value: &Option<FlatVariant>, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    match value {
        None => None,
        Some(value) => indexFV__(value, index),
    }
}

#[doc(hidden)]
pub fn indexFVNN<T>(value: &Option<FlatVariant>, index: Option<T>) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    match value {
        None => None,
        Some(value) => indexFV_N(value, index),
    }
}

// JSON functions and TYPEOF

#[doc(hidden)]
pub fn parse_json_fv_s(value: SqlString) -> FlatVariant {
    serde_json::from_str::<FlatVariant>(value.str()).unwrap_or_default()
}

#[doc(hidden)]
pub fn parse_json_fv_sN(value: Option<SqlString>) -> Option<FlatVariant> {
    value.map(parse_json_fv_s)
}

#[doc(hidden)]
pub fn parse_json_fv_nullN(_value: Option<()>) -> Option<FlatVariant> {
    None
}

#[doc(hidden)]
pub fn to_json_FV(value: FlatVariant) -> Option<SqlString> {
    match value.to_json_string() {
        Ok(s) => Some(SqlString::from(s)),
        _ => None,
    }
}

#[doc(hidden)]
pub fn to_json_FVN(value: Option<FlatVariant>) -> Option<SqlString> {
    value.and_then(to_json_FV)
}

#[doc(hidden)]
pub fn typeof_fv_(value: FlatVariant) -> SqlString {
    SqlString::from_ref(type_string(value.as_bytes()))
}

#[doc(hidden)]
pub fn typeof_fvN(value: Option<FlatVariant>) -> SqlString {
    match value {
        None => SqlString::from_ref("NULL"),
        Some(value) => typeof_fv_(value),
    }
}

#[doc(hidden)]
pub fn variantnull_fv() -> FlatVariant {
    FlatVariant::variant_null()
}

// No from_json_string2: the compiler emits `from_json_string` (variant.rs)
// in both variant modes; its AUX type parameter is generic on every
// implementing type, so FlatVariant programs use it unchanged.
