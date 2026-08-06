//! The runtime functions for [`FlatVariant`] beyond casts: indexing
//! (the VARIANT_INDEX opcode), PARSE_JSON, TO_JSON, TYPEOF, VARIANTNULL,
//! and the JSON_*/VARIANT_* transformation functions.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use crate::flat_variant::casts::{DecodeFV, FVRef, type_string, view};
use crate::flat_variant::{
    Container, FlatVariant, TAG_ARRAY, TAG_BOOLEAN, TAG_DATE, TAG_MAP, TAG_STRING, TAG_TIME,
    TAG_TIMESTAMP, Val, Writer, build_document_infallible, cmp_values, rank,
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

/// The str of an encoded string key, inline or by reference.
fn key_str<'a>(key: Val<'a>) -> &'a str {
    std::str::from_utf8(key.string_bytes()).expect("encoded string must be UTF-8")
}

/// Whether an encoded value is a string, however it is stored.
fn is_string(v: Val<'_>) -> bool {
    rank(v.tag()) == TAG_STRING
}

fn is_integral_numeric(v: Val<'_>) -> bool {
    match view(v) {
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
    keep: fn(Val<'_>) -> bool,
) -> Map<SqlString, Option<T>> {
    let mut result = BTreeMap::<SqlString, Option<T>>::new();
    let doc = value.val();
    if rank(doc.tag()) == TAG_MAP {
        let m = doc.as_map();
        for i in 0..m.count() {
            let key = m.key(i);
            if !is_string(key) {
                continue;
            }
            let val = m.value(i);
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
json_each!(string, SqlString, is_string);
json_each!(boolean, bool, |v: Val<'_>| v.tag() == TAG_BOOLEAN);
json_each!(date, Date, |v: Val<'_>| matches!(
    rank(v.tag()),
    TAG_DATE | TAG_STRING
));
json_each!(time, Time, |v: Val<'_>| matches!(
    rank(v.tag()),
    TAG_TIME | TAG_STRING
));
json_each!(timestamp, Timestamp, |v: Val<'_>| matches!(
    rank(v.tag()),
    TAG_TIMESTAMP | TAG_STRING
));

#[doc(hidden)]
pub fn json_object_keys_FV(value: FlatVariant) -> Array<SqlString> {
    let mut result = Vec::new();
    let doc = value.val();
    if rank(doc.tag()) == TAG_MAP {
        let m = doc.as_map();
        for i in 0..m.count() {
            let key = m.key(i);
            if is_string(key) {
                result.push(SqlString::from_ref(key_str(key)));
            }
        }
    }
    result.into()
}

crate::some_polymorphic_function1!(json_object_keys, FV, FlatVariant, Array<SqlString>);

#[doc(hidden)]
pub fn json_keys_FV(value: FlatVariant) -> Array<SqlString> {
    fn collect(prefix: &str, map: Val<'_>, result: &mut BTreeSet<SqlString>) {
        let m = map.as_map();
        for i in 0..m.count() {
            let key = m.key(i);
            if !is_string(key) {
                continue;
            }
            let path = append_path_component(prefix, key_str(key));
            let val = m.value(i);
            if rank(val.tag()) == TAG_MAP {
                collect(&path, val, result);
            }

            result.insert(SqlString::from(path));
        }
    }
    let mut result = BTreeSet::new();
    let doc = value.val();
    if rank(doc.tag()) == TAG_MAP {
        collect("", doc, &mut result);
    }
    result.into_iter().collect::<Vec<_>>().into()
}

crate::some_polymorphic_function1!(json_keys, FV, FlatVariant, Array<SqlString>);

#[doc(hidden)]
pub fn variant_merge_FV_FV(left: FlatVariant, right: FlatVariant) -> FlatVariant {
    // merge-join over the two sorted key areas.

    /// One output value: a slice of an input for a one-sided key, or the
    /// document built by a recursive merge for a shared key.
    enum Merged<'a> {
        Borrowed(Val<'a>),
        Owned(FlatVariant),
    }
    let (a, b) = (left.val(), right.val());
    if rank(a.tag()) != TAG_MAP || rank(b.tag()) != TAG_MAP {
        return right;
    }
    let (ma, mb) = (a.as_map(), b.as_map());
    if mb.count() == 0 {
        return left;
    }
    let mut entries: Vec<(Val, Merged)> = Vec::with_capacity(ma.count() + mb.count());
    let (mut i, mut j) = (0, 0);
    while i < ma.count() && j < mb.count() {
        let (ka, kb) = (ma.key(i), mb.key(j));
        match cmp_values(ka, kb) {
            Ordering::Less => {
                entries.push((ka, Merged::Borrowed(ma.value(i))));
                i += 1;
            }
            Ordering::Greater => {
                entries.push((kb, Merged::Borrowed(mb.value(j))));
                j += 1;
            }
            // The right key wins
            Ordering::Equal => {
                let merged = variant_merge_FV_FV(
                    FlatVariant::from_val(ma.value(i)),
                    FlatVariant::from_val(mb.value(j)),
                );
                entries.push((kb, Merged::Owned(merged)));
                i += 1;
                j += 1;
            }
        }
    }
    while i < ma.count() {
        entries.push((ma.key(i), Merged::Borrowed(ma.value(i))));
        i += 1;
    }
    while j < mb.count() {
        entries.push((mb.key(j), Merged::Borrowed(mb.value(j))));
        j += 1;
    }
    build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(entries.len());
        for (key, _) in &entries {
            w.copy(*key);
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for (_, val) in &entries {
            match val {
                Merged::Borrowed(v) => w.copy(*v),
                Merged::Owned(d) => w.copy(d.val()),
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
    let doc = value.val();
    if rank(doc.tag()) != TAG_MAP {
        return if predicate_keeps(predicate(&None, &value)) {
            Some(value)
        } else {
            None
        };
    }
    let m = doc.as_map();
    // Kept entries stay sorted, so the result map needs no re-sorting.
    let kept: Vec<usize> = (0..m.count())
        .filter(|&i| {
            let key = Some(FlatVariant::from_val(m.key(i)));
            let val = FlatVariant::from_val(m.value(i));
            predicate_keeps(predicate(&key, &val))
        })
        .collect();
    Some(build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(kept.len());
        for &i in &kept {
            w.copy(m.key(i));
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for &i in &kept {
            w.copy(m.value(i));
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
    let doc = value.val();
    if rank(doc.tag()) != TAG_MAP {
        return mapper(&None, &value).into();
    }
    let m = doc.as_map();
    // Keys are unchanged, so the result map reuses their order.
    let mapped: Vec<FlatVariant> = (0..m.count())
        .map(|i| {
            let key = Some(FlatVariant::from_val(m.key(i)));
            let val = FlatVariant::from_val(m.value(i));
            mapper(&key, &val)
                .into()
                .unwrap_or_else(FlatVariant::sql_null)
        })
        .collect();
    Some(build_document_infallible(|w| {
        let (start, mut key_ends, mut val_ends) = w.begin_map_in_place(m.count());
        for i in 0..m.count() {
            w.copy(m.key(i));
            key_ends.record_end(w);
        }
        w.begin_map_values(&mut val_ends);
        for m in &mapped {
            w.copy(m.val());
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
    let doc = value.val();
    match rank(doc.tag()) {
        TAG_MAP => {
            let m = doc.as_map();
            let mut entries = Vec::with_capacity(m.count());
            for i in 0..m.count() {
                let key = m.key(i);
                let val = FlatVariant::from_val(m.value(i));
                if !is_string(key) {
                    // A non-string key has no path; keep the field untouched
                    let k = w.copy(key);
                    let v = w.copy(val.val());
                    entries.push((k, v));
                    continue;
                }
                let child_path = append_path_component(path, key_str(key));
                let label = Some(SqlString::from_ref(&child_path));
                if predicate_keeps(predicate(&label, &val)) {
                    let k = w.copy(key);
                    let v = deep_filter_encode(w, &child_path, &val, predicate);
                    entries.push((k, v));
                }
            }
            w.map(&entries)
        }
        TAG_ARRAY => {
            let c = Container::new(doc.bytes);
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
        _ => w.copy(doc),
    }
}

#[doc(hidden)]
pub fn variant_deep_filter_fv_<F, B>(value: FlatVariant, predicate: F) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> B,
    B: Into<Option<bool>>,
{
    match value.val().tag() {
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
    let doc = value.val();
    match rank(doc.tag()) {
        TAG_MAP => {
            let m = doc.as_map();
            let mut entries = Vec::with_capacity(m.count());
            for i in 0..m.count() {
                let key = m.key(i);
                let val = FlatVariant::from_val(m.value(i));
                if !is_string(key) {
                    // A non-string key has no path; keep the field untouched
                    let k = w.copy(key);
                    let v = w.copy(val.val());
                    entries.push((k, v));
                    continue;
                }
                let child_path = append_path_component(path, key_str(key));
                let k = w.copy(key);
                let v = deep_map_encode(w, &child_path, &val, mapper);
                entries.push((k, v));
            }
            w.map(&entries)
        }
        TAG_ARRAY => {
            let c = Container::new(doc.bytes);
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
            w.copy(mapped.val())
        }
    }
}

#[doc(hidden)]
pub fn variant_deep_map_fv_<F, R>(value: FlatVariant, mapper: F) -> Option<FlatVariant>
where
    F: Fn(&Option<SqlString>, &FlatVariant) -> R,
    R: Into<Option<FlatVariant>>,
{
    match value.val().tag() {
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
