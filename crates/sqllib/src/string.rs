//! SQL String operations

#![allow(non_snake_case)]
use crate::{
    some_function1, some_function2, some_function3, some_function4, some_polymorphic_function1,
    some_polymorphic_function2, Variant,
};

use core::fmt::Error;
use feldera_types::{deserialize_without_context, serialize_without_context};
use internment::ArcIntern;
use like::{Escape, Like};
use regex::Regex;
use rkyv::Fallible;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use size_of::{Context, SizeOf};
use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SqlString(ArcIntern<String>);

impl SqlString {
    pub fn new() -> Self {
        SqlString(ArcIntern::from_ref(""))
    }

    pub fn from_ref(value: &str) -> Self {
        SqlString(ArcIntern::from_ref(value))
    }

    pub fn str(&self) -> String {
        self.0.clone().to_string()
    }
}

impl Serialize for SqlString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SqlString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        Ok(Self::from_ref(&str))
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
        SqlString(ArcIntern::new(value))
    }
}

impl From<char> for SqlString {
    fn from(value: char) -> Self {
        SqlString(ArcIntern::new(String::from(value)))
    }
}

impl From<&str> for SqlString {
    fn from(value: &str) -> Self {
        SqlString::from(value.to_string())
    }
}

impl SizeOf for SqlString {
    fn size_of_children(&self, context: &mut Context) {
        self.str().size_of_children(context)
    }
}

impl rkyv::Archive for SqlString {
    type Archived = ();
    type Resolver = ();
    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}

impl<D> rkyv::Deserialize<SqlString, D> for ()
where
    D: Fallible + ?Sized,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<SqlString, D::Error> {
        todo!()
    }
}

impl<D> rkyv::Deserialize<SqlString, D> for SqlString
where
    D: Fallible + ?Sized,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<SqlString, D::Error> {
        todo!()
    }
}

impl<S> rkyv::Serialize<S> for SqlString
where
    S: Fallible + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        todo!()
    }
}

///////////////////////////////

pub fn concat_s_s(left: SqlString, right: SqlString) -> SqlString {
    let mut str = left.str();
    str.reserve(right.str().len());
    str.push_str(&right.str());
    SqlString::from(str)
}

some_polymorphic_function2!(concat, s, SqlString, s, SqlString, SqlString);

pub fn substring3___(value: SqlString, left: i32, count: i32) -> SqlString {
    if count < 0 {
        SqlString::new()
    } else {
        // character indexes in SQL start at 1
        let start = if left < 1 { 0 } else { left - 1 };
        let s: String = value
            .str()
            .chars()
            .skip(start as usize)
            .take(count as usize)
            .collect();
        SqlString::from(s)
    }
}

some_function3!(substring3, SqlString, i32, i32, SqlString);

pub fn substring2__(value: SqlString, left: i32) -> SqlString {
    // character indexes in SQL start at 1
    let start = if left < 1 { 0 } else { left - 1 };
    SqlString::from(value.str().chars().skip(start as usize).collect::<String>())
}

some_function2!(substring2, SqlString, i32, SqlString);

pub fn trim_both_s_s(remove: SqlString, value: SqlString) -> SqlString {
    // 'remove' always has exactly 1 character
    let chr = remove.str().chars().next().unwrap();
    SqlString::from(value.str().trim_matches(chr).to_string())
}

some_polymorphic_function2!(trim_both, s, SqlString, s, SqlString, SqlString);

pub fn trim_leading_s_s(remove: SqlString, value: SqlString) -> SqlString {
    // 'remove' always has exactly 1 character
    let chr = remove.str().chars().next().unwrap();
    SqlString::from(value.str().trim_start_matches(chr).to_string())
}

some_polymorphic_function2!(trim_leading, s, SqlString, s, SqlString, SqlString);

pub fn trim_trailing_s_s(remove: SqlString, value: SqlString) -> SqlString {
    // 'remove' always has exactly 1 character
    let chr = remove.str().chars().next().unwrap();
    SqlString::from(value.str().trim_end_matches(chr).to_string())
}

some_polymorphic_function2!(trim_trailing, s, SqlString, s, SqlString, SqlString);

pub fn like2__(value: SqlString, pattern: SqlString) -> bool {
    Like::<false>::like(value.str().as_str(), pattern.str().as_str()).unwrap()
}

some_function2!(like2, SqlString, SqlString, bool);

pub fn rlike__(value: SqlString, pattern: SqlString) -> bool {
    let re = Regex::new(&pattern.str());
    re.map_or_else(|_| false, |re| re.is_match(&value.str()))
}

some_function2!(rlike, SqlString, SqlString, bool);

pub fn like3___(value: SqlString, pattern: SqlString, escape: SqlString) -> bool {
    let escaped = pattern
        .str()
        .as_str()
        .escape(escape.str().as_str())
        .unwrap();
    Like::<true>::like(value.str().as_str(), escaped.as_str()).unwrap()
}

some_function3!(like3, SqlString, SqlString, SqlString, bool);

pub fn position__(needle: SqlString, haystack: SqlString) -> i32 {
    let pos = haystack.str().find(needle.str().as_str());
    match pos {
        None => 0,
        Some(i) => (i + 1) as i32,
    }
}

some_function2!(position, SqlString, SqlString, i32);

pub fn char_length_(value: SqlString) -> i32 {
    value.str().chars().count() as i32
}

some_function1!(char_length, SqlString, i32);

pub fn char_length_ref(value: &str) -> i32 {
    value.chars().count() as i32
}

pub fn ascii_(value: SqlString) -> i32 {
    if value.str().is_empty() {
        0
    } else {
        value.str().chars().next().unwrap() as u32 as i32
    }
}

some_function1!(ascii, SqlString, i32);

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

pub fn repeat__(value: SqlString, count: i32) -> SqlString {
    if count <= 0 {
        SqlString::default()
    } else {
        SqlString::from(value.str().repeat(count as usize))
    }
}

some_function2!(repeat, SqlString, i32, SqlString);

pub fn overlay3___(source: SqlString, replacement: SqlString, position: i32) -> SqlString {
    let len = char_length_ref(&replacement.str());
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, SqlString, SqlString, i32, SqlString);

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
    } else if position > char_length_ref(&source.str()) {
        concat_s_s(source, replacement)
    } else {
        let mut result = substring3___(source.clone(), 0, position - 1).str();
        result += &*(replacement.str());
        result += &substring2__(source, position + remove).str();
        SqlString::from(result)
    }
}

some_function4!(overlay4, SqlString, SqlString, i32, i32, SqlString);

pub fn lower_(source: SqlString) -> SqlString {
    SqlString::from(source.str().to_lowercase())
}

some_function1!(lower, SqlString, SqlString);

pub fn upper_(source: SqlString) -> SqlString {
    SqlString::from(source.str().to_uppercase())
}

some_function1!(upper, SqlString, SqlString);

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

pub fn replace___(haystack: SqlString, needle: SqlString, replacement: SqlString) -> SqlString {
    SqlString::from(haystack.str().replace(&*needle.str(), &replacement.str()))
}

some_function3!(replace, SqlString, SqlString, SqlString, SqlString);

pub fn left__(source: SqlString, size: i32) -> SqlString {
    substring3___(source, 1, size)
}

some_function2!(left, SqlString, i32, SqlString);

pub fn split2__(source: SqlString, separators: SqlString) -> Vec<SqlString> {
    if separators.str().is_empty() {
        return vec![source];
    }
    if source.str().is_empty() {
        return vec![];
    }
    source
        .str()
        .split(&*separators.str())
        .map(SqlString::from)
        .collect()
}

some_function2!(split2, SqlString, SqlString, Vec<SqlString>);

pub fn split1_(source: SqlString) -> Vec<SqlString> {
    split2__(source, SqlString::from_ref(","))
}

some_function1!(split1, SqlString, Vec<SqlString>);

pub fn array_to_string2_vec__(value: Vec<SqlString>, separator: SqlString) -> SqlString {
    SqlString::from(
        value
            .iter()
            .map(|x| x.str())
            .collect::<Vec<String>>()
            .join(&*separator.str()),
    )
}

some_function2!(array_to_string2_vec, Vec<SqlString>, SqlString, SqlString);

pub fn array_to_string2Nvec__(value: Vec<Option<SqlString>>, separator: SqlString) -> SqlString {
    let capacity = value
        .iter()
        .map(|s| s.as_ref().map_or(0, |s| s.str().len()))
        .sum();
    let mut result = String::with_capacity(capacity);
    let mut first = true;
    for word in value {
        let append = match word.as_ref() {
            None => {
                continue;
            }
            Some(r) => r.str(),
        };
        if !first {
            result.push_str(&separator.str())
        }
        first = false;
        result.push_str(append.as_str());
    }
    SqlString::from(result)
}

some_function2!(
    array_to_string2Nvec,
    Vec<Option<SqlString>>,
    SqlString,
    SqlString
);

pub fn array_to_string3_vec___(
    value: Vec<SqlString>,
    separator: SqlString,
    _null_value: SqlString,
) -> SqlString {
    array_to_string2_vec__(value, separator)
}

some_function3!(
    array_to_string3_vec,
    Vec<SqlString>,
    SqlString,
    SqlString,
    SqlString
);

pub fn array_to_string3Nvec___(
    value: Vec<Option<SqlString>>,
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
    for word in value {
        let append = match word.as_ref() {
            None => null_value.str(),
            Some(r) => r.str(),
        };
        if !first {
            result.push_str(&separator.str())
        }
        first = false;
        result.push_str(&append);
    }
    SqlString::from(result)
}

some_function3!(
    array_to_string3Nvec,
    Vec<Option<SqlString>>,
    SqlString,
    SqlString,
    SqlString
);

pub fn writelog<T: std::fmt::Display>(format: SqlString, argument: T) -> T {
    let format_arg = format!("{}", argument);
    let formatted = format.str().replace("%%", &format_arg);
    print!("{}", formatted);
    argument
}

pub fn parse_json_s(value: SqlString) -> Variant {
    match serde_json::from_str::<Variant>(&value.str()) {
        Ok(v) => v,
        Err(_) => Variant::SqlNull,
    }
}

pub fn parse_json_nullN(_value: Option<()>) -> Option<Variant> {
    None
}

some_polymorphic_function1!(parse_json, s, SqlString, Variant);

pub fn to_json_V(value: Variant) -> Option<SqlString> {
    match value.to_json_string() {
        Ok(s) => Some(SqlString::from(s)),
        _ => None,
    }
}

pub fn to_json_VN(value: Option<Variant>) -> Option<SqlString> {
    let value = value?;
    to_json_V(value)
}

pub fn to_json_nullN(_value: Option<()>) -> Option<SqlString> {
    None
}
