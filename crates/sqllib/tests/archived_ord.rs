//! Does comparing two values in their archived form, or an archived value
//! with a decoded one, give the same answer as comparing them decoded?
//!
//! The engine relies on the ordering of archived values being consistent with
//! the ordering over thei deserializes representations for correctness.  This
//! test suite validates this requirement for all supported types.  A merge
//! compares two archived values; a search of a file-backed batch compares an
//! archived value with the decoded key it is looking for, through `OrdRepr`.
//! Both have to agree with the decoded order.
//!
//! The suite has two halves.  The first walks hand-picked values, chosen to
//! test various corner cases (where bugs are the most likely): the extremes of each
//! integer, the special floats, strings that share a prefix, decimals equal in
//! value but not in representation, empty and nested containers, and every arm
//! of the variant enum.  The second says the same thing over random values.

use std::collections::BTreeMap;
use std::mem::size_of;

use dbsp::DBData;
use dbsp::algebra::{F32, F64};
use dbsp::dynamic::{DynData, Erase, HashRepr, OrdRepr, WithFactory};
use dbsp::storage::buffer_cache::FBuf;
use dbsp::storage::file::to_bytes;
use dbsp::utils::tuple::TupleFormat;
use dbsp::utils::{Tup1, Tup2, Tup3, Tup4, Tup5, Tup8, Tup9, Tup10};
use feldera_sqllib::{
    Array, ByteArray, Date, FlatVariant, GeoPoint, LongInterval, Map, ShortInterval, SqlDecimal,
    SqlString, Time, Timestamp, TimestampTz, Uuid, Variant, to_array, to_map,
};
use proptest::prelude::*;

/// Serializes `value` and hands back the buffer holding its archived form.
fn archive<T: DBData>(value: &T) -> FBuf {
    to_bytes(value).expect("serializing a DBData value cannot fail")
}

/// The archived value inside a buffer [`archive`] produced.
///
/// # Safety
///
/// `bytes` must have come from `archive::<T>`.
unsafe fn root<T: DBData>(bytes: &FBuf) -> &T::Repr {
    // SAFETY: the caller promises `bytes` came from `archive::<T>`, which is
    // what `archived_root` needs to find a `T::Repr` at the end of it.
    unsafe { rkyv::archived_root::<T>(bytes.as_slice()) }
}

/// The same archived value, reached the way a merger reaches it: through the
/// factory, as a trait object, with no static knowledge of the concrete type.
///
/// # Safety
///
/// `bytes` must have come from `archive::<T>`.
unsafe fn dyn_root<T>(bytes: &FBuf) -> &<DynData as dbsp::dynamic::ArchiveTrait>::Archived
where
    T: DBData + Erase<DynData>,
    DynData: WithFactory<T>,
{
    let factory = <DynData as WithFactory<T>>::FACTORY;
    // `rkyv` puts the root last, which is how `archived_root` finds it.
    let position = bytes.len() - size_of::<T::Repr>();
    // SAFETY: the caller promises `bytes` came from `archive::<T>`, and
    // `position` is where `rkyv` put the root, computed the same way
    // `archived_root` computes it.
    unsafe { factory.archived_value(bytes.as_slice(), position) }
}

/// Checks the archived comparisons of `a` and `b` against the decoded answer.
///
/// Decoded against decoded is the answer; it is what everything else has to
/// match, not something under test.  Five things are: the archived ordering,
/// the archived equality, the archived ordering again through the trait
/// object a merger holds, which reaches a different implementation from the
/// concrete one and so can disagree with it, and the ordering of an archived
/// value against a decoded one, through `OrdRepr` and again through the
/// trait object, which is the comparison a search of a file-backed batch
/// makes with the key it holds in memory.
fn check_pair<T>(label: &str, a: &T, b: &T)
where
    T: DBData + Erase<DynData>,
    DynData: WithFactory<T>,
{
    let want = a.cmp(b);

    let a_bytes = archive(a);
    let b_bytes = archive(b);
    // SAFETY: both buffers came from `archive::<T>` two lines up.
    let (a_arch, b_arch) = unsafe { (root::<T>(&a_bytes), root::<T>(&b_bytes)) };

    assert_eq!(
        a_arch.cmp(b_arch),
        want,
        "{label}: archived comparison disagrees with decoded\n  left:  {a:?}\n  right: {b:?}"
    );
    assert_eq!(
        (a_arch == b_arch),
        (a == b),
        "{label}: archived equality disagrees with decoded\n  left:  {a:?}\n  right: {b:?}"
    );

    // SAFETY: both buffers came from `archive::<T>` earlier in this function.
    let (a_dyn, b_dyn) = unsafe { (dyn_root::<T>(&a_bytes), dyn_root::<T>(&b_bytes)) };
    assert_eq!(
        a_dyn.cmp(b_dyn),
        want,
        "{label}: archived comparison through the trait object disagrees\n  \
         left:  {a:?}\n  right: {b:?}"
    );

    // Archived against decoded, from both sides, so that an implementation
    // that only agrees with the decoded order from one side shows up.
    assert_eq!(
        a_arch.ord_cmp(b),
        want,
        "{label}: archived-against-decoded comparison disagrees with decoded\n  \
         left:  {a:?}\n  right: {b:?}"
    );
    assert_eq!(
        b_arch.ord_cmp(a),
        want.reverse(),
        "{label}: archived-against-decoded comparison disagrees with decoded\n  \
         left:  {b:?}\n  right: {a:?}"
    );
    assert_eq!(
        a_dyn.cmp_target(b.erase()),
        want,
        "{label}: archived-against-decoded comparison through the trait object disagrees\n  \
         left:  {a:?}\n  right: {b:?}"
    );
    assert_eq!(
        a_dyn.eq_target(b.erase()),
        (a == b),
        "{label}: archived-against-decoded equality through the trait object disagrees\n  \
         left:  {a:?}\n  right: {b:?}"
    );
}

/// Checks every ordered pair drawn from `values`, both ways round.
///
/// Taking both directions is what catches an implementation that is consistent
/// with itself but not antisymmetric, which a one-directional sweep would miss.
fn check_all<T>(label: &str, values: &[T])
where
    T: DBData + Erase<DynData>,
    DynData: WithFactory<T>,
{
    assert!(
        values.len() >= 2,
        "{label}: a single value cannot exercise a comparison"
    );
    for (i, a) in values.iter().enumerate() {
        for (j, b) in values.iter().enumerate() {
            check_pair(&format!("{label}[{i}] vs {label}[{j}]"), a, b);
        }
    }
    check_sort_agrees(label, values);
}

/// Sorting by each ordering must produce the same sequence.
///
/// Pairwise agreement already implies this, but sorting exercises the
/// comparisons in the order a merge would make them and reports a readable
/// sequence when it fails.  The third sort compares each archived value with
/// the decoded form of the other, as a merge of a file batch with an
/// in-memory one does.
fn check_sort_agrees<T>(label: &str, values: &[T])
where
    T: DBData + Erase<DynData>,
    DynData: WithFactory<T>,
{
    let mut by_decoded: Vec<&T> = values.iter().collect();
    by_decoded.sort();

    let archives: Vec<FBuf> = values.iter().map(archive).collect();
    let mut order: Vec<usize> = (0..values.len()).collect();
    // SAFETY: every buffer in `archives` came from `archive::<T>` just above.
    order.sort_by(|&i, &j| unsafe { root::<T>(&archives[i]).cmp(root::<T>(&archives[j])) });
    let by_archived: Vec<&T> = order.into_iter().map(|i| &values[i]).collect();

    assert_eq!(
        by_decoded, by_archived,
        "{label}: sorting by the archived order gives a different sequence"
    );

    let mut order: Vec<usize> = (0..values.len()).collect();
    // SAFETY: every buffer in `archives` came from `archive::<T>` above.
    order.sort_by(|&i, &j| unsafe { root::<T>(&archives[i]) }.ord_cmp(&values[j]));
    let by_mixed: Vec<&T> = order.into_iter().map(|i| &values[i]).collect();

    assert_eq!(
        by_decoded, by_mixed,
        "{label}: sorting archived values against decoded ones gives a different sequence"
    );
}

#[test]
fn integers() {
    check_all("i8", &[i8::MIN, -1, 0, 1, i8::MAX]);
    check_all("i16", &[i16::MIN, -1, 0, 1, i16::MAX]);
    check_all("i32", &[i32::MIN, -1, 0, 1, i32::MAX]);
    check_all("i64", &[i64::MIN, -1, 0, 1, i64::MAX]);
    check_all("i128", &[i128::MIN, -1, 0, 1, i128::MAX]);
    check_all("u8", &[u8::MIN, 1, u8::MAX]);
    check_all("u16", &[u16::MIN, 1, u16::MAX]);
    check_all("u32", &[u32::MIN, 1, u32::MAX]);
    check_all("u64", &[u64::MIN, 1, u64::MAX]);
    check_all("u128", &[u128::MIN, 1, u128::MAX]);
    check_all("bool", &[false, true]);
}

/// Both zeroes, both infinities and NaN: `F32` and `F64` wrap `OrderedFloat`,
/// which gives NaN a place in the order rather than leaving comparisons
/// partial, and the two zeroes are equal numerically but differ in their bits.
fn f32_values() -> Vec<F32> {
    [
        f32::NEG_INFINITY,
        f32::MIN,
        -1.0,
        -0.0,
        0.0,
        f32::MIN_POSITIVE,
        1.0,
        f32::MAX,
        f32::INFINITY,
        f32::NAN,
    ]
    .map(F32::from)
    .to_vec()
}

fn f64_values() -> Vec<F64> {
    [
        f64::NEG_INFINITY,
        f64::MIN,
        -1.0,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        1.0,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
    ]
    .map(F64::from)
    .to_vec()
}

#[test]
fn floats() {
    check_all("F32", &f32_values());
    check_all("F64", &f64_values());
}

/// Strings that share a prefix, differ only in length, or differ only beyond
/// the first byte, which is where a comparison that stops too early shows up.
fn interesting_strings() -> Vec<String> {
    vec![
        String::new(),
        "\0".to_string(),
        "a".to_string(),
        "aa".to_string(),
        "aaa".to_string(),
        "ab".to_string(),
        "b".to_string(),
        "z".to_string(),
        "\u{7f}".to_string(),
        // Multi-byte, and ordered after every ASCII string above.
        "é".to_string(),
        "日本".to_string(),
        "\u{10ffff}".to_string(),
        // An archived string keeps up to 15 bytes inside itself and puts
        // anything longer out of line, behind a relative pointer, so these
        // four sit either side of that switch.  They differ only in length,
        // which is what makes a comparison read to the end of the shorter.
        "y".repeat(14),
        "y".repeat(15),
        "y".repeat(16),
        "y".repeat(17),
        // Sixteen bytes but eight characters, because the switch counts bytes.
        "é".repeat(8),
        "X".repeat(300),
    ]
}

/// The switch the four strings above straddle is really there.
///
/// Without this, a change to `rkyv`'s inlining would quietly leave those
/// cases all on one side of it, and nothing would say so.
#[test]
fn a_long_string_is_stored_out_of_line() {
    let inline = archive(&"y".repeat(15)).len();
    let out_of_line = archive(&"y".repeat(16)).len();
    assert!(
        out_of_line > inline,
        "a 16-byte string archived into {out_of_line} bytes and a 15-byte one into {inline}, \
         so both are stored the same way and the cases above straddle nothing"
    );
}

#[test]
fn strings() {
    check_all("String", &interesting_strings());
    let sql: Vec<SqlString> = interesting_strings()
        .into_iter()
        .map(SqlString::from)
        .collect();
    check_all("SqlString", &sql);
    check_all("char", &['\0', 'a', 'b', 'é', '\u{10ffff}']);
}

/// A byte array orders as a sequence, so a prefix sorts before what extends
/// it, and a longer array is not automatically greater.  It hashes as one
/// too, writing its length before its bytes, which is what the empty array
/// and the pairs differing only in length are here to pin down.
///
/// The payload lives inside the `SmallVec` up to 32 bytes and on the heap
/// beyond it, so the three around that size sit either side of the switch.
fn byte_array_values() -> Vec<ByteArray> {
    vec![
        ByteArray::new(&[]),
        ByteArray::new(&[0]),
        ByteArray::new(&[0, 0]),
        ByteArray::new(&[0, 1]),
        ByteArray::new(&[1]),
        ByteArray::new(&[1, 0]),
        ByteArray::new(&[0xff]),
        ByteArray::new(&[0x7f; 31]),
        ByteArray::new(&[0x7f; 32]),
        ByteArray::new(&[0x7f; 33]),
        ByteArray::new(&vec![0xff; 300]),
    ]
}

#[test]
fn byte_arrays() {
    check_all("ByteArray", &byte_array_values());
}

/// `None` sorts before every `Some`, and hashes its discriminant first, so
/// the archived enum has to keep the variants in that order and write the
/// discriminant at the same width.
fn option_i64_values() -> Vec<Option<i64>> {
    vec![None, Some(i64::MIN), Some(0), Some(i64::MAX)]
}

fn option_string_values() -> Vec<Option<String>> {
    vec![
        None,
        Some(String::new()),
        Some("a".to_string()),
        Some("b".to_string()),
    ]
}

/// Nested, so the discriminant is written twice and the inner one has to be
/// reached through the outer.
fn option_option_i32_values() -> Vec<Option<Option<i32>>> {
    vec![
        None,
        Some(None),
        Some(Some(i32::MIN)),
        Some(Some(0)),
        Some(Some(i32::MAX)),
    ]
}

fn option_f64_values() -> Vec<Option<F64>> {
    vec![
        None,
        Some(F64::from(f64::NEG_INFINITY)),
        Some(F64::from(0.0)),
        Some(F64::from(f64::NAN)),
    ]
}

fn option_sql_string_values() -> Vec<Option<SqlString>> {
    vec![
        None,
        Some(SqlString::from("")),
        Some(SqlString::from("a")),
        Some(SqlString::from("b")),
    ]
}

#[test]
fn options() {
    check_all("Option<i64>", &option_i64_values());
    check_all("Option<String>", &option_string_values());
    check_all("Option<Option<i32>>", &option_option_i32_values());
    check_all("Option<F64>", &option_f64_values());
    check_all("Option<SqlString>", &option_sql_string_values());
}

/// Each temporal type wraps a single integer, so the values worth comparing
/// are the ends of its range and the sign boundary, where a comparison that
/// read the integer as unsigned would go wrong.
fn date_values() -> Vec<Date> {
    [i32::MIN, -1, 0, 1, i32::MAX].map(Date::from_days).to_vec()
}

fn time_values() -> Vec<Time> {
    [0, 1, u64::MAX / 2, u64::MAX]
        .map(Time::from_nanoseconds)
        .to_vec()
}

fn timestamp_values() -> Vec<Timestamp> {
    [i64::MIN, -1, 0, 1, i64::MAX]
        .map(Timestamp::from_microseconds)
        .to_vec()
}

fn timestamp_tz_values() -> Vec<TimestampTz> {
    [i64::MIN, -1, 0, i64::MAX]
        .map(TimestampTz::from_microseconds)
        .to_vec()
}

fn short_interval_values() -> Vec<ShortInterval> {
    [i64::MIN, -1, 0, i64::MAX]
        .map(ShortInterval::from_microseconds)
        .to_vec()
}

fn long_interval_values() -> Vec<LongInterval> {
    [i32::MIN, -1, 0, i32::MAX]
        .map(LongInterval::from_months)
        .to_vec()
}

/// A point orders on its first coordinate and then its second, so these differ
/// in one coordinate at a time and include the special floats each can hold.
fn geo_point_values() -> Vec<GeoPoint> {
    vec![
        GeoPoint::new(f64::NEG_INFINITY, 0.0),
        GeoPoint::new(-1.0, f64::NEG_INFINITY),
        GeoPoint::new(-1.0, 0.0),
        GeoPoint::new(-1.0, 1.0),
        GeoPoint::new(0.0, -1.0),
        GeoPoint::new(0.0, 0.0),
        GeoPoint::new(0.0, f64::NAN),
        GeoPoint::new(1.0, 0.0),
        GeoPoint::new(f64::NAN, 0.0),
    ]
}

fn uuid_values() -> Vec<Uuid> {
    vec![
        Uuid::from_bytes([0x00; 16]),
        Uuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        Uuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]),
        Uuid::from_bytes([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        Uuid::from_bytes([0x80; 16]),
        Uuid::from_bytes([0xff; 16]),
    ]
}

#[test]
fn temporal_and_spatial() {
    check_all("Date", &date_values());
    check_all("Time", &time_values());
    check_all("Timestamp", &timestamp_values());
    check_all("TimestampTz", &timestamp_tz_values());
    check_all("ShortInterval", &short_interval_values());
    check_all("LongInterval", &long_interval_values());
    check_all("GeoPoint", &geo_point_values());
    check_all("Uuid", &uuid_values());
}

/// The values of one decimal type worth comparing: its ends, the sign
/// boundary, and the smallest step it can represent.
fn decimals<const P: usize, const S: usize>() -> Vec<SqlDecimal<P, S>> {
    let one = |mantissa: i128| SqlDecimal::<P, S>::new(mantissa, S as i32).unwrap();
    vec![
        SqlDecimal::<P, S>::MIN,
        one(-100),
        one(-1),
        one(0),
        one(1),
        one(100),
        SqlDecimal::<P, S>::MAX,
    ]
}

#[test]
fn decimals_at_several_scales() {
    // Precision and scale live in the type, so a value has exactly one
    // representation within a type and the orderings can only diverge if the
    // underlying integer is read differently.  Each instantiation is checked
    // separately because each is a distinct archived type.
    check_all("SqlDecimal<10,0>", &decimals::<10, 0>());
    check_all("SqlDecimal<12,2>", &decimals::<12, 2>());
    check_all("SqlDecimal<18,4>", &decimals::<18, 4>());
    check_all("SqlDecimal<28,10>", &decimals::<28, 10>());
    check_all("SqlDecimal<38,0>", &decimals::<38, 0>());
    check_all("SqlDecimal<38,38>", &decimals::<38, 38>());
    check_all("Option<SqlDecimal<38,10>>", &option_decimal_values());
}

fn option_decimal_values() -> Vec<Option<SqlDecimal<38, 10>>> {
    vec![
        None,
        Some(SqlDecimal::<38, 10>::MIN),
        Some(SqlDecimal::<38, 10>::new(0, 10).unwrap()),
        Some(SqlDecimal::<38, 10>::MAX),
    ]
}

/// Sequences that put a prefix beside what extends it, which is where a
/// comparison that looked at length first, or a hash that forgot to write the
/// length at all, would show up.
fn vec_i64_values() -> Vec<Vec<i64>> {
    vec![
        Vec::new(),
        vec![i64::MIN],
        vec![0],
        vec![0, i64::MIN],
        vec![0, 0],
        vec![0, 1],
        vec![1],
        vec![i64::MAX],
        (0..300).collect::<Vec<i64>>(),
    ]
}

fn vec_string_values() -> Vec<Vec<String>> {
    vec![
        Vec::new(),
        vec![String::new()],
        vec!["a".to_string()],
        vec!["a".to_string(), String::new()],
        vec!["a".to_string(), "a".to_string()],
        vec!["b".to_string()],
    ]
}

fn vec_option_i32_values() -> Vec<Vec<Option<i32>>> {
    vec![
        Vec::new(),
        vec![None],
        vec![None, None],
        vec![Some(i32::MIN)],
        vec![Some(0)],
        vec![Some(0), None],
    ]
}

/// Nested, so the element is itself an archived sequence rather than a scalar.
fn vec_vec_u8_values() -> Vec<Vec<Vec<u8>>> {
    vec![
        Vec::new(),
        vec![vec![]],
        vec![vec![0]],
        vec![vec![0], vec![]],
        vec![vec![0, 1]],
        vec![vec![1]],
    ]
}

fn array_sql_string_values() -> Vec<Array<SqlString>> {
    vec![
        to_array(Vec::new()),
        to_array(vec![SqlString::from("")]),
        to_array(vec![SqlString::from("a")]),
        to_array(vec![SqlString::from("a"), SqlString::from("")]),
        to_array(vec![SqlString::from("b")]),
    ]
}

#[test]
fn sequences() {
    check_all("Vec<i64>", &vec_i64_values());
    check_all("Vec<String>", &vec_string_values());
    check_all("Vec<Option<i32>>", &vec_option_i32_values());
    check_all("Vec<Vec<u8>>", &vec_vec_u8_values());
    check_all("Array<SqlString>", &array_sql_string_values());
}

/// Builds a map from pairs, so the cases below stay readable.
fn map_of<K, V>(entries: &[(K, V)]) -> Map<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    to_map(entries.iter().cloned().collect::<BTreeMap<K, V>>())
}

/// A map compares and hashes as its sorted sequence of pairs.  `rkyv` stores
/// it as a B-tree, so an implementation reading the stored layout instead
/// would diverge on maps holding the same entries, and `rkyv`'s own hash
/// leaves out the length prefix that the standard library writes.
fn map_string_i64_values() -> Vec<Map<SqlString, i64>> {
    vec![
        map_of::<SqlString, i64>(&[]),
        map_of(&[(SqlString::from("a"), i64::MIN)]),
        map_of(&[(SqlString::from("a"), 0)]),
        map_of(&[(SqlString::from("a"), 0), (SqlString::from("b"), 0)]),
        map_of(&[(SqlString::from("a"), 1)]),
        map_of(&[(SqlString::from("b"), 0)]),
    ]
}

/// A map whose values are themselves maps, so the recursion goes through a
/// second archived map rather than a scalar.
fn map_nested_values() -> Vec<Map<SqlString, Map<SqlString, i64>>> {
    let inner_empty = map_of::<SqlString, i64>(&[]);
    let inner_one = map_of(&[(SqlString::from("x"), 1i64)]);
    let inner_two = map_of(&[(SqlString::from("x"), 1i64), (SqlString::from("y"), 2)]);
    vec![
        map_of::<SqlString, Map<SqlString, i64>>(&[]),
        map_of(&[(SqlString::from("k"), inner_empty)]),
        map_of(&[(SqlString::from("k"), inner_one.clone())]),
        map_of(&[(SqlString::from("k"), inner_two)]),
        map_of(&[(SqlString::from("l"), inner_one)]),
    ]
}

/// A sequence of maps, the other way of nesting the two containers.
fn vec_of_maps_values() -> Vec<Vec<Map<SqlString, i64>>> {
    vec![
        Vec::new(),
        vec![map_of::<SqlString, i64>(&[])],
        vec![map_of(&[(SqlString::from("a"), 0i64)])],
        vec![map_of(&[(SqlString::from("a"), 0i64)]), map_of(&[])],
    ]
}

/// Plain `BTreeMap`s, so that the map is reached without the reference count
/// the SQL alias wraps it in.
fn btree_map_values() -> Vec<BTreeMap<i64, i64>> {
    vec![
        BTreeMap::new(),
        BTreeMap::from([(1i64, 2i64)]),
        BTreeMap::from([(1i64, 2i64), (3, 4)]),
        BTreeMap::from([(1i64, 3i64)]),
    ]
}

fn btree_map_nested_values() -> Vec<BTreeMap<i64, BTreeMap<i64, i64>>> {
    vec![
        BTreeMap::new(),
        BTreeMap::from([(1i64, BTreeMap::<i64, i64>::new())]),
        BTreeMap::from([(1i64, BTreeMap::from([(2i64, 3i64)]))]),
        BTreeMap::from([(1i64, BTreeMap::from([(2i64, 4i64)]))]),
    ]
}

#[test]
fn maps_and_nesting() {
    check_all("Map<SqlString, i64>", &map_string_i64_values());
    check_all("Map<SqlString, Map<SqlString, i64>>", &map_nested_values());
    check_all("Vec<Map<SqlString, i64>>", &vec_of_maps_values());
    check_all("BTreeMap<i64, i64>", &btree_map_values());
    check_all("BTreeMap nested", &btree_map_nested_values());
}

/// One value of every `Variant` arm, in declaration order, plus a second of
/// several arms so that ordering within an arm is exercised too.
fn variants() -> Vec<Variant> {
    vec![
        Variant::SqlNull,
        Variant::VariantNull,
        Variant::Boolean(false),
        Variant::Boolean(true),
        Variant::TinyInt(i8::MIN),
        Variant::TinyInt(i8::MAX),
        Variant::SmallInt(i16::MIN),
        Variant::Int(i32::MIN),
        Variant::Int(i32::MAX),
        Variant::BigInt(i64::MIN),
        Variant::UTinyInt(0),
        Variant::USmallInt(u16::MAX),
        Variant::UInt(0),
        Variant::UBigInt(u64::MAX),
        Variant::Real(F32::from(f32::NEG_INFINITY)),
        Variant::Real(F32::from(f32::NAN)),
        Variant::Double(F64::from(0.0)),
        Variant::Double(F64::from(f64::NAN)),
        Variant::SqlDecimal((i128::MIN, 0)),
        Variant::SqlDecimal((0, 2)),
        Variant::String(SqlString::from("")),
        Variant::String(SqlString::from("a")),
        Variant::Date(Date::from_days(i32::MIN)),
        Variant::Time(Time::from_nanoseconds(0)),
        Variant::Timestamp(Timestamp::from_microseconds(i64::MIN)),
        Variant::ShortInterval(ShortInterval::from_microseconds(-1)),
        Variant::LongInterval(LongInterval::from_months(-1)),
        Variant::Binary(ByteArray::new(&[])),
        Variant::Binary(ByteArray::new(&[0])),
        Variant::Geometry(GeoPoint::new(0.0, 0.0)),
        Variant::Uuid(Uuid::from_bytes([0; 16])),
        Variant::TimestampTz(TimestampTz::from_microseconds(0)),
    ]
}

#[test]
fn variant_arms() {
    // The arms order by their position in the enum first and by their payload
    // second.  Both sides derive that order, so the two derives have to agree
    // on the variant sequence as well as on every payload type.
    check_all("Variant", &variants());
}

#[test]
fn variant_containers() {
    // The recursive arms: an array of variants and a map keyed by variants.
    // These are the arms `rkyv` boxes out of line, so their comparison goes
    // through a pointer that the archived form resolves differently.
    let leaves = [
        Variant::SqlNull,
        Variant::Int(0),
        Variant::String(SqlString::from("a")),
    ];
    let mut values = vec![
        Variant::Array(to_array(Vec::new())),
        Variant::Map(to_map(BTreeMap::new())),
    ];
    for leaf in &leaves {
        values.push(Variant::Array(to_array(vec![leaf.clone()])));
        values.push(Variant::Array(to_array(vec![leaf.clone(), leaf.clone()])));
        values.push(Variant::Map(to_map(BTreeMap::from([(
            leaf.clone(),
            Variant::Int(1),
        )]))));
    }
    // An array of arrays and a map of maps, so the recursion goes two deep.
    values.push(Variant::Array(to_array(vec![Variant::Array(to_array(
        vec![Variant::Int(0)],
    ))])));
    values.push(Variant::Map(to_map(BTreeMap::from([(
        Variant::Int(0),
        Variant::Map(to_map(BTreeMap::from([(Variant::Int(1), Variant::Int(2))]))),
    )]))));
    check_all("Variant container arms", &values);
}

/// FlatVariant tests.
fn flat_variants() -> Vec<FlatVariant> {
    let mut values: Vec<FlatVariant> = variants().into_iter().map(FlatVariant::from).collect();
    for document in [
        "null",
        "false",
        "true",
        "0",
        "-1",
        "1",
        "1.5",
        r#""""#,
        r#""a""#,
        r#""ab""#,
        "[]",
        "[1]",
        "[1,2]",
        "[[1]]",
        "[[]]",
        "{}",
        r#"{"a":1}"#,
        r#"{"a":1,"b":2}"#,
        r#"{"a":{"b":1}}"#,
        r#"{"ab":1}"#,
        r#"{"a":[{"b":[1,2]}]}"#,
    ] {
        values.push(
            serde_json::from_str(document)
                .unwrap_or_else(|err| panic!("{document} is not valid JSON: {err}")),
        );
    }
    values
}

#[test]
fn flat_variant_documents() {
    check_all("FlatVariant", &flat_variants());
}

#[test]
fn hashing_flat_variant() {
    check_hash_all("FlatVariant", &flat_variants());
}

#[test]
fn tuples() {
    // `Tup1` uses one macro layout and the wider tuples another, so both are
    // covered.  Fields are varied one at a time so that a comparison which
    // stops at the wrong field is visible.
    check_all(
        "Tup1<SqlString>",
        &[
            Tup1::new(SqlString::from("")),
            Tup1::new(SqlString::from("a")),
            Tup1::new(SqlString::from("b")),
        ],
    );
    check_all(
        "Tup2<i8, Option<i16>>",
        &[
            Tup2::new(i8::MIN, None),
            Tup2::new(i8::MIN, Some(i16::MIN)),
            Tup2::new(i8::MIN, Some(i16::MAX)),
            Tup2::new(0, None),
            Tup2::new(i8::MAX, None),
        ],
    );
    check_all(
        "Tup3<SqlString, ByteArray, GeoPoint>",
        &[
            Tup3::new(
                SqlString::from("a"),
                ByteArray::new(&[]),
                GeoPoint::new(0.0, 0.0),
            ),
            Tup3::new(
                SqlString::from("a"),
                ByteArray::new(&[]),
                GeoPoint::new(0.0, 1.0),
            ),
            Tup3::new(
                SqlString::from("a"),
                ByteArray::new(&[1]),
                GeoPoint::new(0.0, 0.0),
            ),
            Tup3::new(
                SqlString::from("b"),
                ByteArray::new(&[]),
                GeoPoint::new(0.0, 0.0),
            ),
        ],
    );
    check_all(
        "Tup4<Timestamp, Date, Time, Uuid>",
        &[
            Tup4::new(
                Timestamp::from_microseconds(i64::MIN),
                Date::from_days(0),
                Time::from_nanoseconds(0),
                Uuid::from_bytes([0; 16]),
            ),
            Tup4::new(
                Timestamp::from_microseconds(i64::MIN),
                Date::from_days(0),
                Time::from_nanoseconds(0),
                Uuid::from_bytes([1; 16]),
            ),
            Tup4::new(
                Timestamp::from_microseconds(0),
                Date::from_days(i32::MIN),
                Time::from_nanoseconds(0),
                Uuid::from_bytes([0; 16]),
            ),
        ],
    );
    check_all(
        "Tup5<Variant, SqlDecimal, F64, Option<SqlString>, Vec<i32>>",
        &[
            Tup5::new(
                Variant::SqlNull,
                SqlDecimal::<12, 2>::new(0, 2).unwrap(),
                F64::from(f64::NAN),
                None,
                Vec::new(),
            ),
            Tup5::new(
                Variant::SqlNull,
                SqlDecimal::<12, 2>::new(0, 2).unwrap(),
                F64::from(f64::NAN),
                None,
                vec![0i32],
            ),
            Tup5::new(
                Variant::SqlNull,
                SqlDecimal::<12, 2>::new(0, 2).unwrap(),
                F64::from(f64::NAN),
                Some(SqlString::from("")),
                Vec::new(),
            ),
            Tup5::new(
                Variant::Int(0),
                SqlDecimal::<12, 2>::MIN,
                F64::from(f64::NEG_INFINITY),
                None,
                Vec::new(),
            ),
        ],
    );
    // Ten fields of mixed width, which is the layout the wide-tuple macro
    // takes; the last field varies so a comparison that stops early fails.
    let wide = |last: u128| Tup10::new(1i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, last);
    check_all("Tup10 mixed widths", &[wide(0), wide(1), wide(u128::MAX)]);
    check_all(
        "Tup10 first field varies",
        &[
            Tup10::new(0i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64, 9i128, 10u128),
            Tup10::new(1i64, 0u32, 0i16, 0u8, 0i8, 0u16, 0i32, 0u64, 0i128, 0u128),
        ],
    );
}

// ---------------------------------------------------------------------------
// Sizes and shapes at which the archived layout changes.
//
// Several of the archived forms are not one layout but a choice among
// layouts, made from the value as it is written.  A map becomes a tree of
// 4 KiB nodes once it outgrows one of them; a wide tuple is written sparsely
// or densely according to how many of its fields are NULL; and a value
// holding one allocation twice is written with that allocation stored once.
// Every value above is small enough, or plain enough, that only the first of
// each pair is ever built, which leaves the code that reads the others
// unreached: the step from one map node to the next, the descent to the first
// entry of a tree, the offsets a sparse tuple holds, and every comparison
// between a value in one layout and a value in the other.
//
// The sizes below were measured rather than assumed, by archiving maps of
// growing size and watching the buffer jump by more than one entry's worth,
// which is a node being added.  Each is checked before it is used, so that a
// change in `rkyv` turns into a failure here instead of into tests that
// quietly stop straddling anything.
// ---------------------------------------------------------------------------

/// Entries in one leaf of an archived `BTreeMap<i64, i64>`.
///
/// A node is filled until it reaches 4 KiB, so the count follows from the
/// size of an entry: 24 bytes of node header and 16 bytes an entry.
const I64_MAP_PER_LEAF: usize = 255;

/// The same for a map keyed by strings short enough to be stored inline,
/// where an entry is 24 bytes.
const INLINE_KEY_MAP_PER_LEAF: usize = 170;

/// And for keys too long for that, which add their own bytes to the node: 24
/// bytes of entry and 40 of key.
const LONG_KEY_MAP_PER_LEAF: usize = 64;

/// Width of a key that an archived string stores out of line.
const LONG_KEY_WIDTH: usize = 40;

/// Width of one it stores inline.
const INLINE_KEY_WIDTH: usize = 6;

/// The size at which a map of long keys grows a third level, so that reaching
/// its first entry descends through two inner nodes rather than one.
///
/// An inner node holds as many keys as a leaf holds entries, so the tree
/// gains a level one leaf past the point where the leaves themselves fill an
/// inner node.
const LONG_KEY_MAP_THREE_LEVELS: usize = (LONG_KEY_MAP_PER_LEAF + 1) * (LONG_KEY_MAP_PER_LEAF + 1);

/// `len` entries, optionally with the value at `changed` replaced.
///
/// Changing a value rather than a key lets two maps be built that first
/// differ at a chosen distance into the sequence, which is how a comparison
/// is made to walk as far as a node boundary instead of deciding at the
/// first entry.
fn i64_map(len: usize, changed: Option<usize>) -> BTreeMap<i64, i64> {
    let mut map: BTreeMap<i64, i64> = (0..len as i64).map(|key| (key, key)).collect();
    if let Some(changed) = changed {
        assert!(
            changed < len,
            "there is no entry {changed} in a map of {len}"
        );
        map.insert(changed as i64, -1);
    }
    map
}

/// The same, keyed by zero-padded strings of `width` bytes.
///
/// Padding to a fixed width keeps the string order and the numeric order the
/// same, so the entry at a given distance into the sequence is the one its
/// number says.
fn string_key_map(len: usize, width: usize, changed: Option<usize>) -> Map<SqlString, i64> {
    let key = |i: usize| SqlString::from(format!("{i:0width$}"));
    let mut map: BTreeMap<SqlString, i64> = (0..len).map(|i| (key(i), i as i64)).collect();
    if let Some(changed) = changed {
        assert!(
            changed < len,
            "there is no entry {changed} in a map of {len}"
        );
        map.insert(key(changed), -1);
    }
    to_map(map)
}

/// Asserts that one more entry cost more than an entry's worth of bytes,
/// which is what a map gaining a node looks like from outside.
fn assert_a_node_was_added<T>(label: &str, entry_bytes: usize, smaller: &T, larger: &T)
where
    T: DBData,
{
    let growth = archive(larger).len() - archive(smaller).len();
    assert!(
        growth > entry_bytes,
        "{label}: one more entry grew the archive by {growth} bytes, no more than the \
         {entry_bytes} an entry takes, so the map did not gain a node and the cases \
         built around this size straddle nothing"
    );
}

/// Maps either side of each size at which the tree gains a leaf, in three
/// versions apiece: unchanged, differing at the first entry, and differing at
/// the last.  Comparing the last pair walks the whole sequence, and so
/// crosses every node boundary the map has.
fn i64_maps_across_node_boundaries() -> Vec<BTreeMap<i64, i64>> {
    let mut values = Vec::new();
    for len in [
        I64_MAP_PER_LEAF - 1,
        I64_MAP_PER_LEAF,
        I64_MAP_PER_LEAF + 1,
        2 * I64_MAP_PER_LEAF + 1,
    ] {
        values.push(i64_map(len, None));
        values.push(i64_map(len, Some(0)));
        values.push(i64_map(len, Some(len - 1)));
    }
    values
}

/// The same either side of the two sizes at which a string-keyed map gains a
/// leaf, which differ because an inlined key and one stored out of line take
/// different amounts of the node.
fn string_key_maps_across_node_boundaries() -> Vec<Map<SqlString, i64>> {
    let mut values = Vec::new();
    for (width, per_leaf) in [
        (INLINE_KEY_WIDTH, INLINE_KEY_MAP_PER_LEAF),
        (LONG_KEY_WIDTH, LONG_KEY_MAP_PER_LEAF),
    ] {
        for len in [per_leaf - 1, per_leaf, per_leaf + 1, 2 * per_leaf + 1] {
            values.push(string_key_map(len, width, None));
            values.push(string_key_map(len, width, Some(len - 1)));
        }
    }
    values
}

/// Maps deep enough to have a level of inner nodes above the level that
/// points at the leaves.
fn three_level_maps() -> Vec<Map<SqlString, i64>> {
    let len = LONG_KEY_MAP_THREE_LEVELS;
    vec![
        string_key_map(len, LONG_KEY_WIDTH, None),
        string_key_map(len, LONG_KEY_WIDTH, Some(0)),
        string_key_map(len, LONG_KEY_WIDTH, Some(len - 1)),
    ]
}

/// Asserts that two maps agree over their first `entries` pairs and disagree
/// somewhere after them.
///
/// A comparison between such a pair cannot be settled inside one node, so it
/// has to step from one to the next; and a hash of either has to read past
/// the first node to tell them apart.  Without this the cases below would
/// still be large, but nothing would say that their comparisons reach any
/// further into them than a two-entry map's does.
fn assert_differs_only_past<K, V>(
    label: &str,
    entries: usize,
    a: &BTreeMap<K, V>,
    b: &BTreeMap<K, V>,
) where
    K: Ord,
    V: PartialEq,
{
    assert!(
        a.iter().take(entries).eq(b.iter().take(entries)),
        "{label}: the two maps already differ within their first {entries} entries, so a \
         comparison between them never leaves the first node"
    );
    assert!(
        a != b,
        "{label}: the two maps are equal, so nothing distinguishes them"
    );
}

/// First, that the sizes above really are the sizes they claim to be, and
/// that the cases built on them compare across a node boundary.
#[test]
fn a_map_gains_a_node_where_the_constants_say() {
    assert_a_node_was_added(
        "BTreeMap<i64, i64>",
        16,
        &i64_map(I64_MAP_PER_LEAF, None),
        &i64_map(I64_MAP_PER_LEAF + 1, None),
    );
    assert_a_node_was_added(
        "Map<SqlString, i64> with inlined keys",
        24,
        &string_key_map(INLINE_KEY_MAP_PER_LEAF, INLINE_KEY_WIDTH, None),
        &string_key_map(INLINE_KEY_MAP_PER_LEAF + 1, INLINE_KEY_WIDTH, None),
    );
    assert_a_node_was_added(
        "Map<SqlString, i64> with keys stored out of line",
        24 + LONG_KEY_WIDTH,
        &string_key_map(LONG_KEY_MAP_PER_LEAF, LONG_KEY_WIDTH, None),
        &string_key_map(LONG_KEY_MAP_PER_LEAF + 1, LONG_KEY_WIDTH, None),
    );
    assert_a_node_was_added(
        "Map<SqlString, i64> deep enough for a third level",
        24 + LONG_KEY_WIDTH,
        &string_key_map(LONG_KEY_MAP_THREE_LEVELS - 1, LONG_KEY_WIDTH, None),
        &string_key_map(LONG_KEY_MAP_THREE_LEVELS, LONG_KEY_WIDTH, None),
    );

    let len = 2 * I64_MAP_PER_LEAF + 1;
    assert_differs_only_past(
        "BTreeMap<i64, i64> spanning nodes",
        I64_MAP_PER_LEAF,
        &i64_map(len, None),
        &i64_map(len, Some(len - 1)),
    );
    let len = 2 * LONG_KEY_MAP_PER_LEAF + 1;
    assert_differs_only_past(
        "Map<SqlString, i64> spanning nodes",
        LONG_KEY_MAP_PER_LEAF,
        &string_key_map(len, LONG_KEY_WIDTH, None),
        &string_key_map(len, LONG_KEY_WIDTH, Some(len - 1)),
    );
    let len = LONG_KEY_MAP_THREE_LEVELS;
    assert_differs_only_past(
        "Map<SqlString, i64> three levels deep",
        LONG_KEY_MAP_PER_LEAF,
        &string_key_map(len, LONG_KEY_WIDTH, None),
        &string_key_map(len, LONG_KEY_WIDTH, Some(len - 1)),
    );
}

#[test]
fn maps_that_span_several_nodes() {
    check_all(
        "BTreeMap<i64, i64> spanning nodes",
        &i64_maps_across_node_boundaries(),
    );
    check_all(
        "Map<SqlString, i64> spanning nodes",
        &string_key_maps_across_node_boundaries(),
    );
    check_all("Map<SqlString, i64> three levels deep", &three_level_maps());
}

#[test]
fn hashing_maps_that_span_several_nodes() {
    check_hash_all(
        "BTreeMap<i64, i64> spanning nodes",
        &i64_maps_across_node_boundaries(),
    );
    check_hash_all(
        "Map<SqlString, i64> spanning nodes",
        &string_key_maps_across_node_boundaries(),
    );
    check_hash_all("Map<SqlString, i64> three levels deep", &three_level_maps());
}

/// A tuple wide enough to take the layout that stores its fields behind a
/// bitmap of which ones are NULL.
type NullableTup10 = Tup10<
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
>;

/// The number of NULL fields at which a ten-field tuple is written sparsely
/// rather than densely: the macro takes the sparse layout once at least 40%
/// of the fields are NULL.
const TUP10_SPARSE_AT: usize = 4;

/// A value holding NULL in the fields named by `nulls`, its own index in
/// every other field, and something else in the field named by `differs`.
///
/// Giving each field a different number is what makes a lookup that returns
/// the wrong field visible: were they all the same, reading the neighbour of
/// the field asked for would give the same answer as reading the right one.
fn nullable_tup10(nulls: &[usize], differs: Option<usize>) -> NullableTup10 {
    let at = |idx: usize| {
        (!nulls.contains(&idx)).then(|| idx as i64 + if differs == Some(idx) { 100 } else { 0 })
    };
    Tup10::new(
        at(0),
        at(1),
        at(2),
        at(3),
        at(4),
        at(5),
        at(6),
        at(7),
        at(8),
        at(9),
    )
}

/// The last field of a tuple of `width` that `nulls` leaves present.
fn last_present(width: usize, nulls: &[usize]) -> Option<usize> {
    (0..width).rev().find(|idx| !nulls.contains(idx))
}

/// Values either side of the sparse threshold, with the NULLs in different
/// places at each count so that the sparse layout's walk from a field index
/// to the offset that holds it is exercised with the set bits spread around
/// rather than always at one end.
///
/// Each shape appears twice, the second differing only in its last present
/// field, so that a comparison between the two has to agree on every field
/// before it and cannot be settled by the first one it reads.
fn nullable_tup10_values() -> Vec<NullableTup10> {
    let none_patterns: Vec<Vec<usize>> = vec![
        // Dense: fewer than four fields are NULL.
        vec![],
        vec![0],
        vec![9],
        vec![0, 1, 2],
        vec![7, 8, 9],
        vec![0, 4, 9],
        // Sparse: four or more are.
        vec![0, 1, 2, 3],
        vec![6, 7, 8, 9],
        vec![0, 2, 4, 6],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
        (0..10).collect(),
    ];
    let mut values = Vec::new();
    for nulls in &none_patterns {
        values.push(nullable_tup10(nulls, None));
        if let Some(last) = last_present(10, nulls) {
            values.push(nullable_tup10(nulls, Some(last)));
        }
    }
    values
}

/// Reads the layout byte an archived wide tuple starts with.
///
/// The field is private, so this reaches it the way `tuple_formats.rs` does.
/// Only a tuple of more than eight fields has one; reading it from a narrower
/// one would return whatever its first byte happens to be.
fn archived_tuple_format<T>(value: &T) -> TupleFormat
where
    T: DBData,
{
    let bytes = archive(value);
    // SAFETY: `bytes` came from `archive::<T>` on the line above.
    let archived = unsafe { root::<T>(&bytes) };
    // SAFETY: `archived` points at a live `T::Repr`, so its first byte is
    // there to read.  It is read as a `u8` rather than as a `TupleFormat`
    // because a narrow tuple has no layout tag, and a byte that is not a
    // discriminant is undefined behaviour to read as the enum; the match
    // below rejects it instead.
    let tag = unsafe { *(archived as *const _ as *const u8) };
    if tag == TupleFormat::Sparse as u8 {
        TupleFormat::Sparse
    } else if tag == TupleFormat::Dense as u8 {
        TupleFormat::Dense
    } else {
        panic!("{tag} is not a layout tag; this is not a tuple wide enough to carry one")
    }
}

/// First, that the values below really do take both layouts.
///
/// Every wide tuple elsewhere in this file holds fields that cannot be NULL,
/// so all of them are written densely and nothing reads a sparse one.
#[test]
fn a_wide_tuple_takes_both_layouts() {
    let below: Vec<usize> = (0..TUP10_SPARSE_AT - 1).collect();
    let at_threshold: Vec<usize> = (0..TUP10_SPARSE_AT).collect();
    assert_eq!(
        archived_tuple_format(&nullable_tup10(&below, None)),
        TupleFormat::Dense,
        "a tuple with {} NULL fields should still be dense",
        TUP10_SPARSE_AT - 1
    );
    assert_eq!(
        archived_tuple_format(&nullable_tup10(&at_threshold, None)),
        TupleFormat::Sparse,
        "a tuple with {TUP10_SPARSE_AT} NULL fields should be sparse"
    );

    let formats: Vec<TupleFormat> = nullable_tup10_values()
        .iter()
        .map(archived_tuple_format)
        .collect();
    assert!(
        formats.contains(&TupleFormat::Dense) && formats.contains(&TupleFormat::Sparse),
        "the values below take only one layout, so no comparison among them reads one \
         of each: {formats:?}"
    );
}

/// Comparing these reaches the sparse layout, and reaches pairs that hold one
/// value in each layout.
#[test]
fn wide_tuples_in_both_layouts() {
    check_all("Tup10 of nullable fields", &nullable_tup10_values());
}

/// The narrowest tuple written behind a bitmap.
type NullableTup9 = Tup9<
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
>;

/// The nine-field counterpart of [`nullable_tup10`].
fn nullable_tup9(nulls: &[usize], differs: Option<usize>) -> NullableTup9 {
    let at = |idx: usize| {
        (!nulls.contains(&idx)).then(|| idx as i64 + if differs == Some(idx) { 100 } else { 0 })
    };
    Tup9::new(
        at(0),
        at(1),
        at(2),
        at(3),
        at(4),
        at(5),
        at(6),
        at(7),
        at(8),
    )
}

/// `rkyv` writes a shared allocation once and points every reference within
/// the same value at it, so a row holding one `Arc` in two columns archives
/// into fewer bytes than a row holding two equal `Arc`s.  SQL arrays and maps
/// are `Arc`-backed, so both shapes arrive in practice, and since the two rows
/// are equal once decoded their archived forms have to compare and hash alike
/// as well.
#[test]
fn a_shared_allocation_compares_as_its_contents() {
    let shared: Array<i64> = to_array(vec![1i64, 2, 3]);
    let separate: Array<i64> = to_array(vec![1i64, 2, 3]);
    let deduplicated = Tup2::new(shared.clone(), shared.clone());
    let written_twice = Tup2::new(shared.clone(), separate);

    assert_eq!(
        deduplicated, written_twice,
        "the two rows should be equal once decoded"
    );
    let (small, large) = (archive(&deduplicated).len(), archive(&written_twice).len());
    assert!(
        small < large,
        "both rows archived into {small} and {large} bytes, so sharing an allocation \
         changed nothing and this case straddles nothing"
    );

    let values = [
        deduplicated,
        written_twice,
        Tup2::new(shared.clone(), to_array(vec![1i64, 2])),
    ];
    check_all("a row sharing an array between two columns", &values);
    check_hash_all("a row sharing an array between two columns", &values);
}

/// A wide tuple of mixed field types, half of them nullable.
///
/// Every other wide tuple here holds `Option<i64>`, so its fields are all the
/// same fixed size.  The sparse layout stores a relative pointer per present
/// field, and the dense one stores each field inline, so both are walked
/// differently once the fields differ in size and some of them keep their
/// payload out of line.
type MixedTup10 = Tup10<
    Option<SqlString>,
    i64,
    Option<F64>,
    Date,
    Option<Vec<i64>>,
    SqlString,
    Option<Timestamp>,
    Uuid,
    Option<i32>,
    bool,
>;

/// NULL in the fields named by `nulls`, and a payload derived from `tag`
/// everywhere else, so that two values of one shape still differ.
fn mixed_tup10(nulls: &[usize], tag: i64) -> MixedTup10 {
    let present = |idx: usize| !nulls.contains(&idx);
    Tup10::new(
        present(0).then(|| SqlString::from(format!("s{tag}"))),
        tag,
        present(2).then(|| F64::from(tag as f64)),
        Date::from_days(tag as i32),
        present(4).then(|| vec![tag]),
        SqlString::from(format!("t{tag}")),
        present(6).then(|| Timestamp::from_microseconds(tag)),
        Uuid::from_bytes([tag as u8; 16]),
        present(8).then_some(tag as i32),
        tag % 2 == 0,
    )
}

/// The five nullable fields are at the even indices, so four of them NULL is
/// 40% of ten and tips the layout over.
fn mixed_tup10_values() -> Vec<MixedTup10> {
    let mut values = Vec::new();
    for nulls in [
        vec![],
        vec![0, 2, 4],
        vec![2, 6, 8],
        vec![0, 2, 4, 6],
        vec![0, 2, 4, 6, 8],
    ] {
        values.push(mixed_tup10(&nulls, 0));
        values.push(mixed_tup10(&nulls, 1));
    }
    values
}

#[test]
fn wide_tuples_of_mixed_types() {
    let formats: Vec<TupleFormat> = mixed_tup10_values()
        .iter()
        .map(archived_tuple_format)
        .collect();
    assert!(
        formats.contains(&TupleFormat::Dense) && formats.contains(&TupleFormat::Sparse),
        "these take only one layout, so no comparison among them reads one of each: {formats:?}"
    );
    check_all("Tup10 of mixed field types", &mixed_tup10_values());
}

/// The macro writes a tuple of up to eight fields one way and a wider one
/// another way, so these sit either side of that switch.
///
/// Nine fields also round the sparse threshold differently from ten: four
/// NULLs of nine is past the 40% mark and three is not, which is a different
/// count from the one `TUP10_SPARSE_AT` names.
#[test]
fn tuples_either_side_of_the_layout_switch() {
    let eight = |last: Option<i64>| Tup8::new(0i64, 1u32, 2i16, 3u8, 4i8, 5u16, 6i32, last);
    check_all(
        "Tup8, the widest of the plain layout",
        &[eight(None), eight(Some(0)), eight(Some(1))],
    );

    assert_eq!(
        archived_tuple_format(&nullable_tup9(&[0, 1, 2], None)),
        TupleFormat::Dense,
        "three NULLs of nine is under 40%, so the tuple should still be dense"
    );
    assert_eq!(
        archived_tuple_format(&nullable_tup9(&[0, 1, 2, 3], None)),
        TupleFormat::Sparse,
        "four NULLs of nine is past 40%, so the tuple should be sparse"
    );
    let mut nine = Vec::new();
    for nulls in [
        vec![],
        vec![0, 1, 2],
        vec![6, 7, 8],
        vec![0, 1, 2, 3],
        vec![5, 6, 7, 8],
        vec![0, 2, 4, 6],
        (0..9).collect::<Vec<_>>(),
    ] {
        nine.push(nullable_tup9(&nulls, None));
        if let Some(last) = last_present(9, &nulls) {
            nine.push(nullable_tup9(&nulls, Some(last)));
        }
    }
    check_all("Tup9, the narrowest of the bitmap layout", &nine);
}

// ---------------------------------------------------------------------------
// Part two: the same agreement, asserted over generated values.
//
// The hand-picked cases above say the orderings agree where someone thought to
// look.  These say it everywhere else.  Every strategy mixes the extremes of
// its type in with random draws.
// ---------------------------------------------------------------------------

/// Asserts the agreement for a generated pair, and for a generated sequence.
///
/// The pair covers every comparison a merge makes.  The sequence covers the
/// same ground in the order a merge would walk it, and reports a whole
/// sequence when it fails, which is usually easier to read than two values.
macro_rules! ordering_proptest {
    ($name:ident, $strategy:expr) => {
        mod $name {
            use super::*;

            proptest! {
                #[test]
                fn pair(a in $strategy, b in $strategy) {
                    check_pair(stringify!($name), &a, &b);
                }
            }

            proptest! {
                #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
                #[test]
                fn sorted(values in prop::collection::vec($strategy, 2..24)) {
                    check_sort_agrees(stringify!($name), &values);
                }
            }
        }
    };
}

fn i64_any() -> BoxedStrategy<i64> {
    prop_oneof![
        Just(i64::MIN),
        Just(-1),
        Just(0),
        Just(1),
        Just(i64::MAX),
        any::<i64>()
    ]
    .boxed()
}

fn i32_any() -> BoxedStrategy<i32> {
    prop_oneof![
        Just(i32::MIN),
        Just(-1),
        Just(0),
        Just(1),
        Just(i32::MAX),
        any::<i32>()
    ]
    .boxed()
}

fn u64_any() -> BoxedStrategy<u64> {
    prop_oneof![Just(0u64), Just(1), Just(u64::MAX), any::<u64>()].boxed()
}

fn i128_any() -> BoxedStrategy<i128> {
    prop_oneof![
        Just(i128::MIN),
        Just(-1),
        Just(0),
        Just(1),
        Just(i128::MAX),
        any::<i128>()
    ]
    .boxed()
}

fn f64_any() -> BoxedStrategy<F64> {
    prop_oneof![
        Just(f64::NEG_INFINITY),
        Just(-0.0f64),
        Just(0.0f64),
        Just(f64::INFINITY),
        Just(f64::NAN),
        any::<f64>(),
    ]
    .prop_map(F64::from)
    .boxed()
}

fn f32_any() -> BoxedStrategy<F32> {
    prop_oneof![
        Just(f32::NEG_INFINITY),
        Just(-0.0f32),
        Just(0.0f32),
        Just(f32::INFINITY),
        Just(f32::NAN),
        any::<f32>(),
    ]
    .prop_map(F32::from)
    .boxed()
}

/// Strings drawn from a tiny alphabet as well as at random, so that generated
/// pairs share prefixes often rather than differing in their first byte.
fn string_any() -> BoxedStrategy<String> {
    prop_oneof![
        Just(String::new()),
        prop::collection::vec(prop::sample::select(vec!['a', 'b', '\0', 'é']), 0..6)
            .prop_map(|v| v.into_iter().collect::<String>()),
        ".{0,32}".prop_map(|s| s),
    ]
    .boxed()
}

fn sql_string_any() -> BoxedStrategy<SqlString> {
    string_any().prop_map(SqlString::from).boxed()
}

fn byte_array_any() -> BoxedStrategy<ByteArray> {
    prop::collection::vec(prop::sample::select(vec![0u8, 1, 0xff]), 0..6)
        .prop_map(|v| ByteArray::new(&v))
        .boxed()
}

fn uuid_any() -> BoxedStrategy<Uuid> {
    prop::array::uniform16(prop::sample::select(vec![0u8, 1, 0x80, 0xff]))
        .prop_map(Uuid::from_bytes)
        .boxed()
}

fn decimal_any<const P: usize, const S: usize>() -> BoxedStrategy<SqlDecimal<P, S>> {
    let min = SqlDecimal::<P, S>::MIN.mantissa();
    let max = SqlDecimal::<P, S>::MAX.mantissa();
    prop_oneof![
        Just(SqlDecimal::<P, S>::MIN),
        Just(SqlDecimal::<P, S>::MAX),
        (min..=max).prop_map(|m| SqlDecimal::<P, S>::new(m, S as i32).unwrap()),
    ]
    .boxed()
}

/// Documents built by nesting, so that generated pairs differ deep inside a
/// value rather than at its first byte.
fn flat_variant_any() -> BoxedStrategy<FlatVariant> {
    let leaf = prop_oneof![
        Just(Variant::SqlNull),
        Just(Variant::VariantNull),
        any::<bool>().prop_map(Variant::Boolean),
        i64_any().prop_map(Variant::BigInt),
        f64_any().prop_map(Variant::Double),
        sql_string_any().prop_map(Variant::String),
    ];
    leaf.prop_recursive(3, 24, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(|v| Variant::Array(to_array(v))),
            prop::collection::btree_map(sql_string_any().prop_map(Variant::String), inner, 0..3)
                .prop_map(|m| Variant::Map(to_map(m))),
        ]
    })
    .prop_map(FlatVariant::from)
    .boxed()
}

ordering_proptest!(prop_flat_variant, flat_variant_any());

ordering_proptest!(prop_i64, i64_any());
ordering_proptest!(prop_u64, u64_any());
ordering_proptest!(prop_i128, i128_any());
ordering_proptest!(prop_f32, f32_any());
ordering_proptest!(prop_f64, f64_any());
ordering_proptest!(prop_string, string_any());
ordering_proptest!(prop_sql_string, sql_string_any());
ordering_proptest!(prop_byte_array, byte_array_any());
ordering_proptest!(prop_uuid, uuid_any());
ordering_proptest!(prop_option_i64, proptest::option::of(i64_any()));
ordering_proptest!(prop_option_string, proptest::option::of(string_any()));
ordering_proptest!(
    prop_option_option_i32,
    proptest::option::of(proptest::option::of(i32_any()))
);
ordering_proptest!(prop_decimal_12_2, decimal_any::<12, 2>());
ordering_proptest!(prop_decimal_38_10, decimal_any::<38, 10>());
ordering_proptest!(prop_date, i32_any().prop_map(Date::from_days));
ordering_proptest!(prop_time, u64_any().prop_map(Time::from_nanoseconds));
ordering_proptest!(
    prop_timestamp,
    i64_any().prop_map(Timestamp::from_microseconds)
);
ordering_proptest!(
    prop_timestamp_tz,
    i64_any().prop_map(TimestampTz::from_microseconds)
);
ordering_proptest!(
    prop_short_interval,
    i64_any().prop_map(ShortInterval::from_microseconds)
);
ordering_proptest!(
    prop_long_interval,
    i32_any().prop_map(LongInterval::from_months)
);
ordering_proptest!(
    prop_geo_point,
    (f64_any(), f64_any()).prop_map(|(x, y)| GeoPoint::new(x.into_inner(), y.into_inner()))
);
ordering_proptest!(prop_vec_i64, prop::collection::vec(i64_any(), 0..6));
ordering_proptest!(prop_vec_string, prop::collection::vec(string_any(), 0..4));
ordering_proptest!(
    prop_vec_option_i32,
    prop::collection::vec(proptest::option::of(i32_any()), 0..5)
);
ordering_proptest!(
    prop_vec_vec_u8,
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4), 0..4)
);
ordering_proptest!(
    prop_map_string_i64,
    prop::collection::btree_map(sql_string_any(), i64_any(), 0..4).prop_map(to_map)
);
ordering_proptest!(
    prop_map_nested,
    prop::collection::btree_map(
        sql_string_any(),
        prop::collection::btree_map(sql_string_any(), i64_any(), 0..3).prop_map(to_map),
        0..3
    )
    .prop_map(to_map)
);
ordering_proptest!(
    prop_vec_of_maps,
    prop::collection::vec(
        prop::collection::btree_map(sql_string_any(), i64_any(), 0..3).prop_map(to_map),
        0..3
    )
);
ordering_proptest!(
    prop_tup3_nested,
    (
        proptest::option::of(i64_any()),
        prop::collection::vec(sql_string_any(), 0..3),
        f64_any(),
    )
        .prop_map(|(a, b, c)| Tup3::new(a, b, c))
);
ordering_proptest!(
    prop_array_sql_string,
    prop::collection::vec(sql_string_any(), 0..4).prop_map(to_array)
);
ordering_proptest!(
    prop_tup2,
    (i32_any(), proptest::option::of(string_any())).prop_map(|(a, b)| Tup2::new(a, b))
);
ordering_proptest!(
    prop_tup3_sqllib,
    (sql_string_any(), byte_array_any(), uuid_any()).prop_map(|(a, b, c)| Tup3::new(a, b, c))
);
ordering_proptest!(
    prop_tup5_mixed,
    (
        f64_any(),
        decimal_any::<18, 4>(),
        proptest::option::of(sql_string_any()),
        prop::collection::vec(i32_any(), 0..3),
        i64_any(),
    )
        .prop_map(|(a, b, c, d, e)| Tup5::new(a, b, c, d, e))
);

/// Ten nullable fields drawn independently, so that generated pairs land in
/// both layouts and in every mixture of the two, with the NULLs wherever the
/// draw puts them rather than where a hand-written case thought to.
fn nullable_tup10_any() -> BoxedStrategy<NullableTup10> {
    prop::collection::vec(proptest::option::of(i64_any()), 10)
        .prop_map(|fields| {
            Tup10::new(
                fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6],
                fields[7], fields[8], fields[9],
            )
        })
        .boxed()
}

ordering_proptest!(prop_tup10_nullable, nullable_tup10_any());

/// The mixed-type wide tuple, drawn at random, so that the NULLs land
/// wherever the draw puts them and the sized and out-of-line fields mix.
fn mixed_tup10_any() -> BoxedStrategy<MixedTup10> {
    (
        proptest::option::of(sql_string_any()),
        i64_any(),
        proptest::option::of(f64_any()),
        i32_any().prop_map(Date::from_days),
        proptest::option::of(prop::collection::vec(i64_any(), 0..3)),
        sql_string_any(),
        proptest::option::of(i64_any().prop_map(Timestamp::from_microseconds)),
        uuid_any(),
        proptest::option::of(i32_any()),
        any::<bool>(),
    )
        .prop_map(|(a, b, c, d, e, f, g, h, i, j)| Tup10::new(a, b, c, d, e, f, g, h, i, j))
        .boxed()
}

ordering_proptest!(prop_tup10_mixed, mixed_tup10_any());

// ---------------------------------------------------------------------------
// Hashing.
//
// Splicing a run of keys into a merge output has to feed the output's
// membership filter, which hashes every key.  A spliced key is never decoded,
// so that hash has to come from the archived form and has to equal what the
// decoded form would produce: the filter is written here and queried later
// from a decoded key, and a mismatch is a false negative, which a membership
// filter is never allowed to produce.  The lookup then finds nothing and the
// query is silently wrong.
//
// Meeting that requirement took hashing the archived form directly rather
// than deferring to `rkyv`'s own `Hash`, which disagrees with the standard
// library in two places: it leaves out the length prefix a map writes, and it
// gives an enum a discriminant narrower than the decoded one.
// ---------------------------------------------------------------------------

/// A hasher that records what it was asked to write rather than a hash.
///
/// Every `write_*` is left on its default, which routes through `write`, so
/// the log tells one write of a slice's bytes from one write an element.  The
/// hasher the engine uses cannot: it is insensitive to where one call ends
/// and the next begins, so it answers the same either way.  Hashing
/// faithfully is the stronger claim of making the same calls, and this is
/// what checks it.
#[derive(Default)]
struct CallLog(Vec<Vec<u8>>);

impl std::hash::Hasher for CallLog {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.push(bytes.to_vec());
    }
}

/// The calls hashing `value` makes, in order.
fn calls_of(hash: impl FnOnce(&mut CallLog)) -> Vec<Vec<u8>> {
    let mut log = CallLog::default();
    hash(&mut log);
    log.0
}

/// Checks that an archived value hashes exactly as its decoded form does:
/// the same answer, and the same calls to get there.
fn check_hash<T>(label: &str, values: &[T])
where
    T: DBData,
    T::Repr: HashRepr,
{
    for (i, value) in values.iter().enumerate() {
        let bytes = archive(value);
        // SAFETY: `bytes` came from `archive::<T>` on the line above.
        let archived = unsafe { root::<T>(&bytes) };
        assert_eq!(
            dbsp::dynamic::archived_hash(archived),
            Some(dbsp::default_hash(value)),
            "{label}[{i}]: the archived form hashes differently from the decoded one\n  \
             value: {value:?}"
        );
        assert_eq!(
            calls_of(|log| std::hash::Hash::hash(value, log)),
            calls_of(|log| archived.hash_repr(log)),
            "{label}[{i}]: the archived form asks the hasher for something different \
             from what the decoded one asks for\n  value: {value:?}"
        );
    }
}

/// Checks that a type's archived form claims to be faithful and hashes the
/// way the decoded form does, over every value given.
fn check_hash_all<T>(label: &str, values: &[T])
where
    T: DBData + HashRepr,
    T::Repr: HashRepr,
{
    assert!(
        <T::Repr as HashRepr>::FAITHFUL,
        "{label}: the archived form does not claim to hash faithfully"
    );
    check_hash(label, values);
}

#[test]
fn hashing_scalars() {
    check_hash_all("i64", &[i64::MIN, -1, 0, 1, i64::MAX]);
    check_hash_all("u128", &[0u128, 1, u128::MAX]);
    check_hash_all("bool", &[false, true]);
    check_hash_all("String", &interesting_strings());
    let sql: Vec<SqlString> = interesting_strings()
        .into_iter()
        .map(SqlString::from)
        .collect();
    check_hash_all("SqlString", &sql);
    check_hash_all("F32", &f32_values());
    check_hash_all("F64", &f64_values());
    check_hash_all("i8", &[i8::MIN, -1, 0, 1, i8::MAX]);
    check_hash_all("i32", &[i32::MIN, -1, 0, 1, i32::MAX]);
    check_hash_all("i128", &[i128::MIN, -1, 0, 1, i128::MAX]);
    check_hash_all("u8", &[u8::MIN, 1, u8::MAX]);
    check_hash_all("char", &['\0', 'a', 'b', 'é', '\u{10ffff}']);
}

/// A decimal archives to itself, so the two hashes are the same code and
/// these cases say that the archived form really is the decoded one, at every
/// precision and scale and after a round trip through the archive.
#[test]
fn hashing_decimals() {
    check_hash_all("SqlDecimal<10,0>", &decimals::<10, 0>());
    check_hash_all("SqlDecimal<12,2>", &decimals::<12, 2>());
    check_hash_all("SqlDecimal<18,4>", &decimals::<18, 4>());
    check_hash_all("SqlDecimal<28,10>", &decimals::<28, 10>());
    check_hash_all("SqlDecimal<38,0>", &decimals::<38, 0>());
    check_hash_all("SqlDecimal<38,38>", &decimals::<38, 38>());
    check_hash_all("Option<SqlDecimal<38,10>>", &option_decimal_values());
}

/// A byte array does not archive to itself, and the length it writes before
/// its bytes is the part that can go missing.
#[test]
fn hashing_byte_arrays() {
    check_hash_all("ByteArray", &byte_array_values());
}

#[test]
fn hashing_temporal_and_spatial() {
    check_hash_all("Date", &date_values());
    check_hash_all("Time", &time_values());
    check_hash_all("Timestamp", &timestamp_values());
    check_hash_all("TimestampTz", &timestamp_tz_values());
    check_hash_all("ShortInterval", &short_interval_values());
    check_hash_all("LongInterval", &long_interval_values());
    check_hash_all("GeoPoint", &geo_point_values());
    check_hash_all("Uuid", &uuid_values());
}

/// Two of the things the ordering half covers this one cannot: the legacy
/// variant and tuples of more than eight fields.  Neither reproduces the
/// decoded hash yet, so both decline, and a caller reads the decline and
/// decodes the value instead.
///
/// Declining is an answer, not a missing implementation.  Both archived forms
/// implement the trait, so a caller bounded on `Archived<K>: HashRepr`
/// compiles for them and takes the fallback; without that it would fail to
/// build for any key of nine or more columns, and for every VARIANT.
#[test]
fn the_types_that_decline_say_so_rather_than_answer() {
    let wide = Tup9::new(0i64, 1i64, 2i64, 3i64, 4i64, 5i64, 6i64, 7i64, 8i64);
    let bytes = archive(&wide);
    // SAFETY: `bytes` came from `archive` on the line above.
    let archived = unsafe { root::<Tup9<i64, i64, i64, i64, i64, i64, i64, i64, i64>>(&bytes) };
    assert_eq!(dbsp::dynamic::archived_hash(archived), None);

    let variant = Variant::String(SqlString::from("a"));
    let bytes = archive(&variant);
    // SAFETY: `bytes` came from `archive` on the line above.
    let archived = unsafe { root::<Variant>(&bytes) };
    assert_eq!(dbsp::dynamic::archived_hash(archived), None);

    // And declining spreads, so a container of one declines too.
    let held = vec![Variant::String(SqlString::from("a"))];
    let bytes = archive(&held);
    // SAFETY: `bytes` came from `archive` on the line above.
    let archived = unsafe { root::<Vec<Variant>>(&bytes) };
    assert_eq!(dbsp::dynamic::archived_hash(archived), None);
}

/// The same containers the ordering tests walk, hashed.
///
/// Drawing from the same lists is deliberate: the two halves would otherwise
/// drift, and the container cases are where they diverge most easily, since a
/// hash has a length prefix and a discriminant width to get right that an
/// ordering does not.
#[test]
fn hashing_containers() {
    check_hash_all("Option<i64>", &option_i64_values());
    check_hash_all("Option<String>", &option_string_values());
    check_hash_all("Option<Option<i32>>", &option_option_i32_values());
    check_hash_all("Option<F64>", &option_f64_values());
    check_hash_all("Option<SqlString>", &option_sql_string_values());

    check_hash_all("Vec<i64>", &vec_i64_values());
    check_hash_all("Vec<String>", &vec_string_values());
    check_hash_all("Vec<Option<i32>>", &vec_option_i32_values());
    check_hash_all("Vec<Vec<u8>>", &vec_vec_u8_values());
    check_hash_all("Array<SqlString>", &array_sql_string_values());

    // The maps are the cases `rkyv`'s own `Hash` gets wrong, by leaving out
    // the length prefix that the standard library writes.
    check_hash_all("Map<SqlString, i64>", &map_string_i64_values());
    check_hash_all("Map<SqlString, Map<SqlString, i64>>", &map_nested_values());
    check_hash_all("Vec<Map<SqlString, i64>>", &vec_of_maps_values());
    check_hash_all("BTreeMap<i64, i64>", &btree_map_values());
    check_hash_all("BTreeMap nested", &btree_map_nested_values());
}

#[test]
fn hashing_tuples() {
    check_hash_all(
        "Tup2<i64, SqlString>",
        &[
            Tup2::new(0i64, SqlString::from("")),
            Tup2::new(0i64, SqlString::from("a")),
            Tup2::new(1i64, SqlString::from("")),
        ],
    );
    check_hash_all(
        "Tup3 with options",
        &[
            Tup3::new(
                Some(1i64),
                Option::<SqlString>::None,
                Timestamp::from_microseconds(0),
            ),
            Tup3::new(
                None,
                Some(SqlString::from("a")),
                Timestamp::from_microseconds(-1),
            ),
        ],
    );
    // Eight fields is the widest tuple whose archived form hashes at all: a
    // wider one has no archived hash, faithful or otherwise.
    check_hash_all(
        "Tup8, the widest that hashes",
        &[
            Tup8::new(0i64, 1u32, 2i16, 3u8, 4i8, 5u16, 6i32, None::<i64>),
            Tup8::new(0i64, 1u32, 2i16, 3u8, 4i8, 5u16, 6i32, Some(0i64)),
            Tup8::new(1i64, 0u32, 0i16, 0u8, 0i8, 0u16, 0i32, Some(i64::MIN)),
        ],
    );
    check_hash_all(
        "Tup5 mixed",
        &[
            Tup5::new(
                F64::from(f64::NAN),
                Date::from_days(0),
                SqlString::from("a"),
                vec![1i64],
                Uuid::from_bytes([0; 16]),
            ),
            Tup5::new(
                F64::from(0.0),
                Date::from_days(-1),
                SqlString::from("b"),
                Vec::new(),
                Uuid::from_bytes([1; 16]),
            ),
        ],
    );
}

macro_rules! hashing_proptest {
    ($name:ident, $strategy:expr) => {
        mod $name {
            use super::*;

            proptest! {
                #[test]
                fn hashes_alike(value in $strategy) {
                    check_hash(stringify!($name), std::slice::from_ref(&value));
                }
            }
        }
    };
}

hashing_proptest!(hash_flat_variant, flat_variant_any());
hashing_proptest!(hash_i64, i64_any());
hashing_proptest!(hash_u64, u64_any());
hashing_proptest!(hash_f64, f64_any());
hashing_proptest!(hash_string, string_any());
hashing_proptest!(hash_sql_string, sql_string_any());
hashing_proptest!(hash_option_i64, proptest::option::of(i64_any()));
hashing_proptest!(hash_option_string, proptest::option::of(string_any()));
hashing_proptest!(hash_vec_i64, prop::collection::vec(i64_any(), 0..6));
hashing_proptest!(hash_vec_string, prop::collection::vec(string_any(), 0..4));
hashing_proptest!(
    hash_option_option_i32,
    proptest::option::of(proptest::option::of(i32_any()))
);
hashing_proptest!(
    hash_vec_option_i32,
    prop::collection::vec(proptest::option::of(i32_any()), 0..5)
);
hashing_proptest!(
    hash_vec_vec_u8,
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4), 0..4)
);
hashing_proptest!(
    hash_array_sql_string,
    prop::collection::vec(sql_string_any(), 0..4).prop_map(to_array)
);
hashing_proptest!(
    hash_vec_of_maps,
    prop::collection::vec(
        prop::collection::btree_map(sql_string_any(), i64_any(), 0..3).prop_map(to_map),
        0..3
    )
);
hashing_proptest!(
    hash_map_string_i64,
    prop::collection::btree_map(sql_string_any(), i64_any(), 0..4)
);
hashing_proptest!(
    hash_map_nested,
    prop::collection::btree_map(
        i64_any(),
        prop::collection::btree_map(i64_any(), i64_any(), 0..3),
        0..3
    )
);
hashing_proptest!(hash_i128, i128_any());
hashing_proptest!(hash_f32, f32_any());
hashing_proptest!(hash_uuid, uuid_any());
hashing_proptest!(hash_byte_array, byte_array_any());
hashing_proptest!(hash_decimal_12_2, decimal_any::<12, 2>());
hashing_proptest!(hash_decimal_38_10, decimal_any::<38, 10>());
hashing_proptest!(hash_time, u64_any().prop_map(Time::from_nanoseconds));
hashing_proptest!(
    hash_timestamp_tz,
    i64_any().prop_map(TimestampTz::from_microseconds)
);
hashing_proptest!(
    hash_short_interval,
    i64_any().prop_map(ShortInterval::from_microseconds)
);
hashing_proptest!(
    hash_long_interval,
    i32_any().prop_map(LongInterval::from_months)
);
hashing_proptest!(hash_date, i32_any().prop_map(Date::from_days));
hashing_proptest!(
    hash_timestamp,
    i64_any().prop_map(Timestamp::from_microseconds)
);
hashing_proptest!(
    hash_geo_point,
    (f64_any(), f64_any()).prop_map(|(x, y)| GeoPoint::new(x.into_inner(), y.into_inner()))
);
hashing_proptest!(
    hash_tup2,
    (i64_any(), sql_string_any()).prop_map(|(a, b)| Tup2::new(a, b))
);
hashing_proptest!(
    hash_tup3_nested,
    (
        proptest::option::of(i64_any()),
        prop::collection::vec(sql_string_any(), 0..3),
        f64_any(),
    )
        .prop_map(|(a, b, c)| Tup3::new(a, b, c))
);
