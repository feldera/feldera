//! The runtime functions for [`FlatVariant`] beyond casts: indexing
//! (the VARIANT_INDEX opcode), PARSE_JSON, TO_JSON, TYPEOF, VARIANTNULL,
//! and the JSON_*/VARIANT_* transformation functions.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use crate::flat_variant::casts::{DecodeFV, FVRef, type_string, view};
use crate::flat_variant::{
    Container, FlatVariant, TAG_ARRAY, TAG_BOOLEAN, TAG_DATE, TAG_MAP, TAG_STRING, TAG_TIME,
    TAG_TIMESTAMP, Writer, build_document_infallible, cmp_values,
};
// The path-label helpers are shared with the enum implementation; they
// move here when Variant is deprecated.
use crate::variant::{append_path_component, predicate_keeps};
use crate::{Array, Date, Map, SqlString, Time, Timestamp};

// Indexing (VARIANT_INDEX opcode), native on the flat encoding

// Return type is always Option<FlatVariant>, matching the indexV grid.
#[doc(hidden)]
pub fn indexFV__<T>(value: &FlatVariant, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    value.index_from_one(&index.into())
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

/// The str of an encoded string key.
fn key_str(key: &[u8]) -> &str {
    std::str::from_utf8(&key[1..]).expect("encoded string must be UTF-8")
}

fn is_integral_numeric(bytes: &[u8]) -> bool {
    match view(bytes) {
        FVRef::TinyInt(_)
        | FVRef::SmallInt(_)
        | FVRef::Int(_)
        | FVRef::BigInt(_)
        | FVRef::UTinyInt(_)
        | FVRef::USmallInt(_)
        | FVRef::UInt(_)
        | FVRef::UBigInt(_) => true,
        FVRef::Real(x) => x.into_inner().fract() == 0.0,
        FVRef::Double(x) => x.into_inner().fract() == 0.0,
        FVRef::Decimal(sig, scale) => match 10i128.checked_pow(scale as u32) {
            Some(divisor) => sig % divisor == 0,
            None => sig == 0,
        },
        _ => false,
    }
}

fn json_each_typed<T: DecodeFV>(
    value: &FlatVariant,
    keep: fn(&[u8]) -> bool,
) -> Map<SqlString, Option<T>> {
    let mut result = BTreeMap::<SqlString, Option<T>>::new();
    let bytes = value.as_bytes();
    if bytes[0] == TAG_MAP {
        let c = Container::new(bytes);
        for i in 0..c.count {
            let key = &bytes[c.element(i)];
            if key[0] != TAG_STRING {
                continue;
            }
            let val = &bytes[c.map_value(i)];
            if !keep(val) {
                continue;
            }
            if let Ok(converted) = T::decode(val) {
                result.insert(SqlString::from_ref(key_str(key)), Some(converted));
            }
        }
    }
    result.into()
}

macro_rules! json_each {
    ($type_name:ident, $type:ty, $keep:expr) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<json_each_ $type_name _FV>](value: FlatVariant) -> Map<SqlString, Option<$type>> {
                json_each_typed(&value, $keep)
            }

            crate::some_polymorphic_function1!([<json_each_ $type_name>], FV, FlatVariant, Map<SqlString, Option<$type>>);
        }
    };
}

json_each!(bigint, i64, is_integral_numeric);
json_each!(string, SqlString, |v| v[0] == TAG_STRING);
json_each!(boolean, bool, |v| v[0] == TAG_BOOLEAN);
json_each!(date, Date, |v| matches!(v[0], TAG_DATE | TAG_STRING));
json_each!(time, Time, |v| matches!(v[0], TAG_TIME | TAG_STRING));
json_each!(timestamp, Timestamp, |v| matches!(
    v[0],
    TAG_TIMESTAMP | TAG_STRING
));

#[doc(hidden)]
pub fn json_object_keys_FV(value: FlatVariant) -> Array<SqlString> {
    let mut result = Vec::new();
    let bytes = value.as_bytes();
    if bytes[0] == TAG_MAP {
        let c = Container::new(bytes);
        for i in 0..c.count {
            let key = &bytes[c.element(i)];
            if key[0] == TAG_STRING {
                result.push(SqlString::from_ref(key_str(key)));
            }
        }
    }
    result.into()
}

crate::some_polymorphic_function1!(json_object_keys, FV, FlatVariant, Array<SqlString>);

#[doc(hidden)]
pub fn json_keys_FV(value: FlatVariant) -> Array<SqlString> {
    fn collect(prefix: &str, map: &[u8], result: &mut BTreeSet<SqlString>) {
        let c = Container::new(map);
        for i in 0..c.count {
            let key = &map[c.element(i)];
            if key[0] != TAG_STRING {
                continue;
            }
            let path = append_path_component(prefix, key_str(key));
            let val = &map[c.map_value(i)];
            if val[0] == TAG_MAP {
                collect(&path, val, result);
            }

            result.insert(SqlString::from(path));
        }
    }
    let mut result = BTreeSet::new();
    let bytes = value.as_bytes();
    if bytes[0] == TAG_MAP {
        collect("", bytes, &mut result);
    }
    result.into_iter().collect::<Vec<_>>().into()
}

crate::some_polymorphic_function1!(json_keys, FV, FlatVariant, Array<SqlString>);

#[doc(hidden)]
pub fn variant_merge_FV_FV(left: FlatVariant, right: FlatVariant) -> FlatVariant {
    // merge-join over the two sorted key areas.

    /// One output value: a slice of an input for a one-sided key, or the
    /// document built by a recursive merge for a shared key.
    enum Val<'a> {
        Slice(&'a [u8]),
        Doc(FlatVariant),
    }
    let (a, b) = (left.as_bytes(), right.as_bytes());
    if a[0] != TAG_MAP || b[0] != TAG_MAP {
        return right;
    }
    let (ca, cb) = (Container::new(a), Container::new(b));
    if cb.count == 0 {
        return left;
    }
    let mut entries: Vec<(&[u8], Val)> = Vec::with_capacity(ca.count + cb.count);
    let (mut i, mut j) = (0, 0);
    while i < ca.count && j < cb.count {
        let ka = &a[ca.element(i)];
        let kb = &b[cb.element(j)];
        match cmp_values(ka, kb) {
            Ordering::Less => {
                entries.push((ka, Val::Slice(&a[ca.map_value(i)])));
                i += 1;
            }
            Ordering::Greater => {
                entries.push((kb, Val::Slice(&b[cb.map_value(j)])));
                j += 1;
            }
            // The right key wins
            Ordering::Equal => {
                let merged = variant_merge_FV_FV(
                    left.subvalue(ca.map_value(i)),
                    right.subvalue(cb.map_value(j)),
                );
                entries.push((kb, Val::Doc(merged)));
                i += 1;
                j += 1;
            }
        }
    }
    while i < ca.count {
        entries.push((&a[ca.element(i)], Val::Slice(&a[ca.map_value(i)])));
        i += 1;
    }
    while j < cb.count {
        entries.push((&b[cb.element(j)], Val::Slice(&b[cb.map_value(j)])));
        j += 1;
    }
    build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(entries.len());
        for (key, _) in &entries {
            w.raw(key);
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for (_, val) in &entries {
            match val {
                Val::Slice(s) => w.raw(s),
                Val::Doc(d) => w.raw(d.as_bytes()),
            };
            val_ends.record_end(w);
        }
        start..w.out.len()
    })
}

crate::some_polymorphic_function2!(variant_merge, FV, FlatVariant, FV, FlatVariant, FlatVariant);

#[doc(hidden)]
pub fn variant_filter_fv_<F, B>(value: FlatVariant, predicate: F) -> Option<FlatVariant>
where
    F: Fn(&Option<FlatVariant>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    let bytes = value.as_bytes();
    if bytes[0] != TAG_MAP {
        return if predicate_keeps(predicate(&None, &value)) {
            Some(value)
        } else {
            None
        };
    }
    let c = Container::new(bytes);
    // Kept entries stay sorted, so the result map needs no re-sorting.
    let kept: Vec<usize> = (0..c.count)
        .filter(|&i| {
            let key = Some(value.subvalue(c.element(i)));
            let val = value.subvalue(c.map_value(i));
            predicate_keeps(predicate(&key, &val))
        })
        .collect();
    Some(build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(kept.len());
        for &i in &kept {
            w.raw(&bytes[c.element(i)]);
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for &i in &kept {
            w.raw(&bytes[c.map_value(i)]);
            val_ends.record_end(w);
        }
        start..w.out.len()
    }))
}

#[doc(hidden)]
pub fn variant_filter_fvN<F, B>(value: Option<FlatVariant>, predicate: F) -> Option<FlatVariant>
where
    F: Fn(&Option<FlatVariant>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    variant_filter_fv_(value?, predicate)
}

#[doc(hidden)]
pub fn variant_map_fv_<F, R>(value: FlatVariant, mapper: F) -> Option<FlatVariant>
where
    F: Fn(&Option<FlatVariant>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    let bytes = value.as_bytes();
    if bytes[0] != TAG_MAP {
        return mapper(&None, &value).into();
    }
    let c = Container::new(bytes);
    // Keys are unchanged, so the result map reuses their order.
    let mapped: Vec<FlatVariant> = (0..c.count)
        .map(|i| {
            let key = Some(value.subvalue(c.element(i)));
            let val = value.subvalue(c.map_value(i));
            mapper(&key, &val)
                .into()
                .unwrap_or_else(FlatVariant::sql_null)
        })
        .collect();
    Some(build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(c.count);
        for i in 0..c.count {
            w.raw(&bytes[c.element(i)]);
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for m in &mapped {
            w.raw(m.as_bytes());
            val_ends.record_end(w);
        }
        start..w.out.len()
    }))
}

#[doc(hidden)]
pub fn variant_map_fvN<F, R>(value: Option<FlatVariant>, mapper: F) -> Option<FlatVariant>
where
    F: Fn(&Option<FlatVariant>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    variant_map_fv_(value?, mapper)
}

fn deep_filter_encode<F, B>(
    w: &mut Writer,
    path: &str,
    value: &FlatVariant,
    predicate: &F,
) -> Range<usize>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    // The kept count is unknown, so children are encoded
    // first and the container is assembled from their ranges.
    let bytes = value.as_bytes();
    match bytes[0] {
        TAG_MAP => {
            let c = Container::new(bytes);
            let mut entries = Vec::with_capacity(c.count);
            for i in 0..c.count {
                let key = &bytes[c.element(i)];
                let val = value.subvalue(c.map_value(i));
                if key[0] != TAG_STRING {
                    // A non-string key has no path; keep the field untouched
                    let k = w.raw(key);
                    let v = w.raw(val.as_bytes());
                    entries.push((k, v));
                    continue;
                }
                let child_path = append_path_component(path, key_str(key));
                let label = Some(SqlString::from_ref(&child_path));
                if predicate_keeps(predicate(&label, &val)) {
                    let k = w.raw(key);
                    let v = deep_filter_encode(w, &child_path, &val, predicate);
                    entries.push((k, v));
                }
            }
            w.map(&entries)
        }
        TAG_ARRAY => {
            let c = Container::new(bytes);
            let mut children = Vec::new();
            for i in 0..c.count {
                let val = value.subvalue(c.element(i));
                // SQL array indexes start from 1
                let child_path = format!("{path}[{}]", i + 1);
                let label = Some(SqlString::from_ref(&child_path));
                if predicate_keeps(predicate(&label, &val)) {
                    children.push(deep_filter_encode(w, &child_path, &val, predicate));
                }
            }
            w.array(&children)
        }
        _ => w.raw(bytes),
    }
}

#[doc(hidden)]
pub fn variant_deep_filter_fv_<F, B>(value: FlatVariant, predicate: F) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    match value.as_bytes()[0] {
        TAG_MAP | TAG_ARRAY => Some(build_document_infallible(|w| {
            deep_filter_encode(w, "", &value, &predicate)
        })),
        _ => {
            if predicate_keeps(predicate(&None, &value)) {
                Some(value)
            } else {
                None
            }
        }
    }
}

#[doc(hidden)]
pub fn variant_deep_filter_fvN<F, B>(
    value: Option<FlatVariant>,
    predicate: F,
) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    variant_deep_filter_fv_(value?, predicate)
}

fn deep_map_encode<F, R>(
    w: &mut Writer,
    path: &str,
    value: &FlatVariant,
    mapper: &F,
) -> Range<usize>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    let bytes = value.as_bytes();
    match bytes[0] {
        TAG_MAP => {
            let c = Container::new(bytes);
            let mut entries = Vec::with_capacity(c.count);
            for i in 0..c.count {
                let key = &bytes[c.element(i)];
                let val = value.subvalue(c.map_value(i));
                if key[0] != TAG_STRING {
                    // A non-string key has no path; keep the field untouched
                    let k = w.raw(key);
                    let v = w.raw(val.as_bytes());
                    entries.push((k, v));
                    continue;
                }
                let child_path = append_path_component(path, key_str(key));
                let k = w.raw(key);
                let v = deep_map_encode(w, &child_path, &val, mapper);
                entries.push((k, v));
            }
            w.map(&entries)
        }
        TAG_ARRAY => {
            let c = Container::new(bytes);
            let mut children = Vec::with_capacity(c.count);
            for i in 0..c.count {
                let val = value.subvalue(c.element(i));
                // SQL array indexes start from 1
                let child_path = format!("{path}[{}]", i + 1);
                children.push(deep_map_encode(w, &child_path, &val, mapper));
            }
            w.array(&children)
        }
        _ => {
            let label = Some(SqlString::from_ref(path));
            let mapped = mapper(&label, value)
                .into()
                .unwrap_or_else(FlatVariant::sql_null);
            w.raw(mapped.as_bytes())
        }
    }
}

#[doc(hidden)]
pub fn variant_deep_map_fv_<F, R>(value: FlatVariant, mapper: F) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    match value.as_bytes()[0] {
        TAG_MAP | TAG_ARRAY => Some(build_document_infallible(|w| {
            deep_map_encode(w, "", &value, &mapper)
        })),
        _ => mapper(&None, &value).into(),
    }
}

#[doc(hidden)]
pub fn variant_deep_map_fvN<F, R>(value: Option<FlatVariant>, mapper: F) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    variant_deep_map_fv_(value?, mapper)
}
