//! SQL String operations.
//!

// SqlString is implemented based on the ArcStr crate.
// The module contains various string operations.

#![allow(non_snake_case)]
use crate::{
    array::Array, some_function1, some_function2, some_function3, some_function4,
    some_polymorphic_function1, some_polymorphic_function2, string_interner::*, Variant,
};

use core::fmt::Error;
use feldera_types::{deserialize_without_context, serialize_without_context};
use like::{Escape, Like};
use md5::{Digest, Md5};
use regex::Regex;
use rkyv::{
    string::{ArchivedString, StringResolver},
    DeserializeUnsized, Fallible, SerializeUnsized,
};
use serde::{Deserialize, Serialize};
use size_of::{Context, SizeOf};
use std::{
    cmp::{max, min},
    fmt::{Display, Formatter},
    sync::Arc,
};

use arcstr::ArcStr;

type StringRef = ArcStr;
pub type InternedString = InternedStringId;

/// An immutable reference counted string.
#[derive(Clone, Default, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SqlString(StringRef);

/// String representation used by the Feldera SQL runtime
impl SqlString {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_ref(value: &str) -> Self {
        SqlString(StringRef::from(value.to_string()))
    }

    pub fn str(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

serialize_without_context!(SqlString);
deserialize_without_context!(SqlString);

impl Display for SqlString {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.str().fmt(f)
    }
}

impl From<String> for SqlString {
    fn from(value: String) -> Self {
        SqlString(StringRef::from(value))
    }
}

impl From<char> for SqlString {
    fn from(value: char) -> Self {
        SqlString(StringRef::from(String::from(value)))
    }
}

impl From<&str> for SqlString {
    fn from(value: &str) -> Self {
        SqlString::from_ref(value)
    }
}

impl SizeOf for SqlString {
    fn size_of_children(&self, context: &mut Context) {
        self.0.size_of_children(context);
    }
}

impl rkyv::Archive for SqlString {
    type Archived = ArchivedString;
    type Resolver = StringResolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ArchivedString::resolve_from_str(self.str(), pos, resolver, out);
    }
}

impl<S: Fallible + ?Sized> rkyv::Serialize<S> for SqlString
where
    str: SerializeUnsized<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        ArchivedString::serialize_from_str(self.str(), serializer)
    }
}

impl<D: Fallible + ?Sized> rkyv::Deserialize<SqlString, D> for ArchivedString
where
    str: DeserializeUnsized<str, D>,
{
    #[inline]
    fn deserialize(&self, _: &mut D) -> Result<SqlString, D::Error> {
        Ok(SqlString::from_ref(self.as_str()))
    }
}

#[doc(hidden)]
pub fn concat_s_s(left: SqlString, right: SqlString) -> SqlString {
    let mut result = String::with_capacity(left.len() + right.len());
    result.push_str(left.str());
    result.push_str(right.str());
    SqlString::from(result)
}

some_polymorphic_function2!(concat, s, SqlString, s, SqlString, SqlString);

#[doc(hidden)]
pub fn substring3___(value: SqlString, left: i32, count: i32) -> SqlString {
    if count < 0 {
        SqlString::new()
    } else {
        // character indexes in SQL start at 1
        let start = if left < 1 { 0 } else { left - 1 } as usize;
        let count = max(0, count) as usize;
        let start = min(value.len(), start);
        let end = min(value.len(), start + count);
        SqlString::from_ref(&value.str()[start..end])
    }
}

some_function3!(substring3, SqlString, i32, i32, SqlString);

#[doc(hidden)]
pub fn substring2__(value: SqlString, left: i32) -> SqlString {
    // character indexes in SQL start at 1
    let start = if left < 1 { 0 } else { left - 1 } as usize;
    let start = min(start, value.len());
    SqlString::from_ref(&value.str()[start..])
}

some_function2!(substring2, SqlString, i32, SqlString);

#[doc(hidden)]
pub fn trim_both_s_s(remove: SqlString, value: SqlString) -> SqlString {
    SqlString::from(value.str().trim_matches(|c| remove.str().contains(c)))
}

some_polymorphic_function2!(trim_both, s, SqlString, s, SqlString, SqlString);

#[doc(hidden)]
pub fn trim_leading_s_s(remove: SqlString, value: SqlString) -> SqlString {
    SqlString::from(value.str().trim_start_matches(|c| remove.str().contains(c)))
}

some_polymorphic_function2!(trim_leading, s, SqlString, s, SqlString, SqlString);

#[doc(hidden)]
pub fn trim_trailing_s_s(remove: SqlString, value: SqlString) -> SqlString {
    SqlString::from(value.str().trim_end_matches(|c| remove.str().contains(c)))
}

some_polymorphic_function2!(trim_trailing, s, SqlString, s, SqlString, SqlString);

#[doc(hidden)]
pub fn like2__(value: SqlString, pattern: SqlString) -> bool {
    Like::<false>::like(value.str(), pattern.str()).unwrap()
}

some_function2!(like2, SqlString, SqlString, bool);

// rlike with a Constant regular expression.
// re is None when the regular expression expression is malformed,
// In this case the result is false and not None.
// The regular expression cannot be null - the compiler would detect that.
#[doc(hidden)]
pub fn rlikeC__(value: SqlString, re: &Option<Regex>) -> bool {
    match re {
        None => false,
        Some(re) => re.is_match(value.str()),
    }
}

#[doc(hidden)]
pub fn rlikeCN_(value: Option<SqlString>, re: &Option<Regex>) -> Option<bool> {
    let value = value?;
    Some(rlikeC__(value, re))
}

#[doc(hidden)]
pub fn rlike__(value: SqlString, pattern: SqlString) -> bool {
    let re = Regex::new(pattern.str());
    re.map_or_else(|_| false, |re| re.is_match(value.str()))
}

some_function2!(rlike, SqlString, SqlString, bool);

#[doc(hidden)]
pub fn like3___(value: SqlString, pattern: SqlString, escape: SqlString) -> bool {
    let escaped = pattern.str().escape(escape.str()).unwrap();
    Like::<true>::like(value.str(), &escaped).unwrap()
}

some_function3!(like3, SqlString, SqlString, SqlString, bool);

#[doc(hidden)]
pub fn ilike2__(value: SqlString, pattern: SqlString) -> bool {
    // Convert both the value and the pattern to lowercase for case-insensitive comparison
    Like::<false>::like(
        value.str().to_lowercase().as_str(),
        pattern.str().to_lowercase().as_str(),
    )
    .unwrap()
}

some_function2!(ilike2, SqlString, SqlString, bool);

#[doc(hidden)]
pub fn position__(needle: SqlString, haystack: SqlString) -> i32 {
    let pos = haystack.str().find(needle.str());
    match pos {
        None => 0,
        Some(i) => (i + 1) as i32,
    }
}

some_function2!(position, SqlString, SqlString, i32);

#[doc(hidden)]
pub fn char_length_(value: SqlString) -> i32 {
    value.str().chars().count() as i32
}

some_function1!(char_length, SqlString, i32);

#[doc(hidden)]
pub fn char_length_ref(value: &str) -> i32 {
    value.chars().count() as i32
}

#[doc(hidden)]
pub fn ascii_(value: SqlString) -> i32 {
    if value.str().is_empty() {
        0
    } else {
        value.str().chars().next().unwrap() as u32 as i32
    }
}

some_function1!(ascii, SqlString, i32);

#[doc(hidden)]
pub fn chr_(code: i32) -> SqlString {
    if code < 0 {
        SqlString::default()
    } else {
        let c = char::from_u32(code as u32);
        match c {
            None => SqlString::default(),
            Some(v) => SqlString::from(v),
        }
    }
}

some_function1!(chr, i32, SqlString);

#[doc(hidden)]
pub fn repeat__(value: SqlString, count: i32) -> SqlString {
    if count <= 0 {
        SqlString::default()
    } else {
        SqlString::from(value.str().repeat(count as usize))
    }
}

some_function2!(repeat, SqlString, i32, SqlString);

#[doc(hidden)]
pub fn overlay3___(source: SqlString, replacement: SqlString, position: i32) -> SqlString {
    let len = char_length_ref(replacement.str());
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, SqlString, SqlString, i32, SqlString);

#[doc(hidden)]
pub fn overlay4____(
    source: SqlString,
    replacement: SqlString,
    position: i32,
    remove: i32,
) -> SqlString {
    let mut remove = remove;
    if remove < 0 {
        remove = 0;
    }
    if position <= 0 {
        source
    } else if position > char_length_ref(source.str()) {
        concat_s_s(source, replacement)
    } else {
        let mut result = substring3___(source.clone(), 0, position - 1).to_string();
        result += replacement.str();
        result += substring2__(source, position + remove).str();
        SqlString::from(result)
    }
}

some_function4!(overlay4, SqlString, SqlString, i32, i32, SqlString);

#[doc(hidden)]
pub fn lower_(source: SqlString) -> SqlString {
    SqlString::from(source.str().to_lowercase())
}

some_function1!(lower, SqlString, SqlString);

#[doc(hidden)]
pub fn upper_(source: SqlString) -> SqlString {
    SqlString::from(source.str().to_uppercase())
}

some_function1!(upper, SqlString, SqlString);

#[doc(hidden)]
pub fn initcap_(source: SqlString) -> SqlString {
    let mut result = String::with_capacity(source.str().len());
    let mut capitalize_next = true;
    for c in source.str().chars() {
        if c.is_alphanumeric() {
            if capitalize_next {
                for r in c.to_uppercase() {
                    result.push(r);
                }
                capitalize_next = false;
            } else {
                for r in c.to_lowercase() {
                    result.push(r);
                }
                capitalize_next = false;
            }
        } else {
            capitalize_next = true;
            result.push(c);
        }
    }
    SqlString::from(result)
}

some_function1!(initcap, SqlString, SqlString);

#[doc(hidden)]
pub fn replace___(haystack: SqlString, needle: SqlString, replacement: SqlString) -> SqlString {
    SqlString::from(haystack.str().replace(needle.str(), replacement.str()))
}

some_function3!(replace, SqlString, SqlString, SqlString, SqlString);

#[doc(hidden)]
pub fn left_s_i32(source: SqlString, size: i32) -> SqlString {
    substring3___(source, 1, size)
}

some_polymorphic_function2!(left, s, SqlString, i32, i32, SqlString);

#[doc(hidden)]
pub fn right_s_i32(source: SqlString, size: i32) -> SqlString {
    if size <= 0 {
        return SqlString::new();
    }
    let size = size as usize;
    let start = if size >= source.len() {
        1
    } else {
        source.len() - size + 1
    };
    substring3___(source, start as i32, size as i32)
}

some_polymorphic_function2!(right, s, SqlString, i32, i32, SqlString);

#[doc(hidden)]
pub fn split2__(source: SqlString, separators: SqlString) -> Array<SqlString> {
    if separators.str().is_empty() {
        return Arc::new(vec![source]);
    }
    if source.str().is_empty() {
        return Arc::new(vec![]);
    }
    source
        .str()
        .split(separators.str())
        .map(SqlString::from)
        .collect::<Vec<SqlString>>()
        .into()
}

some_function2!(split2, SqlString, SqlString, Array<SqlString>);

#[doc(hidden)]
pub fn split1_(source: SqlString) -> Array<SqlString> {
    split2__(source, SqlString::from_ref(","))
}

some_function1!(split1, SqlString, Array<SqlString>);

#[doc(hidden)]
pub fn split_part___(s: SqlString, delimiter: SqlString, n: i32) -> SqlString {
    let parts: Array<SqlString> = split2__(s, delimiter);
    let part_count = parts.len() as i32;

    // Handle negative indices
    let n = if n < 0 { part_count + n + 1 } else { n };

    if n <= 0 || n > part_count {
        return SqlString::new();
    }

    parts[(n - 1) as usize].clone()
}

some_function3!(split_part, SqlString, SqlString, i32, SqlString);

#[doc(hidden)]
pub fn array_to_string2_vec__(value: Array<SqlString>, separator: SqlString) -> SqlString {
    (*value)
        .iter()
        .map(|x| x.str())
        .collect::<Vec<&str>>()
        .join(separator.str())
        .into()
}

some_function2!(array_to_string2_vec, Array<SqlString>, SqlString, SqlString);

#[doc(hidden)]
pub fn array_to_string2Nvec__(value: Array<Option<SqlString>>, separator: SqlString) -> SqlString {
    let capacity = value
        .iter()
        .map(|s| s.as_ref().map_or(0, |s| s.str().len()))
        .sum();
    let mut result = String::with_capacity(capacity);
    let mut first = true;
    for word in &*value {
        let append = match word.as_ref() {
            None => {
                continue;
            }
            Some(r) => r.str(),
        };
        if !first {
            result.push_str(separator.str())
        }
        first = false;
        result.push_str(append);
    }
    SqlString::from(result)
}

some_function2!(
    array_to_string2Nvec,
    Array<Option<SqlString>>,
    SqlString,
    SqlString
);

#[doc(hidden)]
pub fn array_to_string3_vec___(
    value: Array<SqlString>,
    separator: SqlString,
    _null_value: SqlString,
) -> SqlString {
    array_to_string2_vec__(value, separator)
}

some_function3!(
    array_to_string3_vec,
    Array<SqlString>,
    SqlString,
    SqlString,
    SqlString
);

#[doc(hidden)]
pub fn array_to_string3Nvec___(
    value: Array<Option<SqlString>>,
    separator: SqlString,
    null_value: SqlString,
) -> SqlString {
    let null_size = null_value.str().len();
    let capacity = value
        .iter()
        .map(|s| s.as_ref().map_or(null_size, |s| s.str().len()))
        .sum();
    let mut result = String::with_capacity(capacity);
    let mut first = true;
    for word in &*value {
        let append = match word.as_ref() {
            None => null_value.str(),
            Some(r) => r.str(),
        };
        if !first {
            result.push_str(separator.str())
        }
        first = false;
        result.push_str(append);
    }
    SqlString::from(result)
}

some_function3!(
    array_to_string3Nvec,
    Array<Option<SqlString>>,
    SqlString,
    SqlString,
    SqlString
);

#[doc(hidden)]
pub fn writelog<T: std::fmt::Display>(format: SqlString, argument: T) -> T {
    let format_arg = format!("{}", argument);
    let formatted = format.str().replace("%%", &format_arg);
    print!("{}", formatted);
    argument
}

#[doc(hidden)]
pub fn parse_json_s(value: SqlString) -> Variant {
    match serde_json::from_str::<Variant>(value.str()) {
        Ok(v) => v,
        Err(_) => Variant::SqlNull,
    }
}

#[doc(hidden)]
pub fn parse_json_nullN(_value: Option<()>) -> Option<Variant> {
    None
}

some_polymorphic_function1!(parse_json, s, SqlString, Variant);

#[doc(hidden)]
pub fn to_json_V(value: Variant) -> Option<SqlString> {
    match value.to_json_string() {
        Ok(s) => Some(SqlString::from(s)),
        _ => None,
    }
}

#[doc(hidden)]
pub fn to_json_VN(value: Option<Variant>) -> Option<SqlString> {
    let value = value?;
    to_json_V(value)
}

#[doc(hidden)]
pub fn to_json_nullN(_value: Option<()>) -> Option<SqlString> {
    None
}

#[doc(hidden)]
pub fn regexp_replace3___(str: SqlString, re: SqlString, repl: SqlString) -> SqlString {
    let re = Regex::new(re.str()).ok();
    regexp_replaceC3___(str, &re, repl)
}

some_function3!(regexp_replace3, SqlString, SqlString, SqlString, SqlString);

#[doc(hidden)]
pub fn regexp_replace2__(str: SqlString, re: SqlString) -> SqlString {
    regexp_replace3___(str, re, SqlString::new())
}

some_function2!(regexp_replace2, SqlString, SqlString, SqlString);

#[doc(hidden)]
pub fn regexp_replaceC3___(str: SqlString, re: &Option<Regex>, repl: SqlString) -> SqlString {
    match re {
        None => str,
        Some(re) => SqlString::from_ref(re.replace_all(str.str(), repl.str()).as_ref()),
    }
}

#[doc(hidden)]
pub fn regexp_replaceC3N__(
    str: Option<SqlString>,
    re: &Option<Regex>,
    repl: SqlString,
) -> Option<SqlString> {
    let str = str?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC3__N(
    str: SqlString,
    re: &Option<Regex>,
    repl: Option<SqlString>,
) -> Option<SqlString> {
    let repl = repl?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC3N_N(
    str: Option<SqlString>,
    re: &Option<Regex>,
    repl: Option<SqlString>,
) -> Option<SqlString> {
    let str = str?;
    let repl = repl?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC2__(str: SqlString, re: &Option<Regex>) -> SqlString {
    regexp_replaceC3___(str, re, SqlString::new())
}

#[doc(hidden)]
pub fn regexp_replaceC2N_(str: Option<SqlString>, re: &Option<Regex>) -> Option<SqlString> {
    let str = str?;
    Some(regexp_replaceC3___(str, re, SqlString::new()))
}

#[doc(hidden)]
pub fn concat_ws___(sep: SqlString, left: SqlString, right: SqlString) -> SqlString {
    let mut result = String::with_capacity(left.len() + right.len() + sep.len());
    result.push_str(left.str());
    result.push_str(sep.str());
    result.push_str(right.str());
    SqlString::from(result)
}

#[doc(hidden)]
pub fn concat_wsN__(
    sep: Option<SqlString>,
    left: SqlString,
    right: SqlString,
) -> Option<SqlString> {
    let sep = sep?;
    Some(concat_ws___(sep, left, right))
}

#[doc(hidden)]
pub fn concat_ws_N_(sep: SqlString, left: Option<SqlString>, right: SqlString) -> SqlString {
    match left {
        None => right,
        Some(left) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_ws_NN(
    sep: SqlString,
    left: Option<SqlString>,
    right: Option<SqlString>,
) -> SqlString {
    match (left, right) {
        (None, None) => SqlString::new(),
        (None, Some(right)) => right,
        (Some(left), None) => left,
        (Some(left), Some(right)) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_wsNN_(
    sep: Option<SqlString>,
    left: Option<SqlString>,
    right: SqlString,
) -> Option<SqlString> {
    let sep = sep?;
    Some(concat_ws_N_(sep, left, right))
}

#[doc(hidden)]
pub fn concat_ws__N(sep: SqlString, left: SqlString, right: Option<SqlString>) -> SqlString {
    match right {
        None => left,
        Some(right) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_wsN_N(
    sep: Option<SqlString>,
    left: SqlString,
    right: Option<SqlString>,
) -> Option<SqlString> {
    let sep = sep?;
    Some(concat_ws__N(sep, left, right))
}

#[doc(hidden)]
pub fn concat_wsNNN(
    sep: Option<SqlString>,
    left: Option<SqlString>,
    right: Option<SqlString>,
) -> Option<SqlString> {
    let sep = sep?;
    Some(concat_ws_NN(sep, left, right))
}

#[doc(hidden)]
#[cfg(test)]
mod tests {
    use crate::SqlString;
    use dbsp::storage::file::to_bytes;
    use rkyv::from_bytes;
    use size_of::SizeOf;

    #[test]
    fn rkyv_serialize_deserialize_sqlstring() {
        let s = SqlString::from_ref("abc1✅");
        let archived = to_bytes(&s).unwrap();
        let deserialized: SqlString = from_bytes(&archived).unwrap();
        assert_eq!(s.str(), deserialized.str());
    }

    #[test]
    fn sizeof_sqlstring() {
        let s = SqlString::from_ref("abcdefghijklmnopqrstuvwxyz");
        let total_size = SizeOf::size_of(&s);

        // The exact size may depend on the architecture's pointer size.
        assert!(total_size.total_bytes() > 26);
        assert!(total_size.shared_bytes() >= 26);
    }
}

#[doc(hidden)]
pub fn md5_s(source: SqlString) -> SqlString {
    let mut hasher = Md5::new();
    hasher.update(source.str());
    let result = hasher.finalize();
    SqlString::from(format!("{:x}", result))
}

some_polymorphic_function1!(md5, s, SqlString, SqlString);

pub fn intern(s: Option<SqlString>) -> Option<InternedString> {
    s.map(|s| intern_string(&s))
}

pub fn unintern(id: Option<InternedString>) -> Option<SqlString> {
    match id {
        None => None,
        Some(id) => unintern_string(&id),
    }
}
