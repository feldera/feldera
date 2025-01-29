//! SQL String operations

#![allow(non_snake_case)]
use crate::{
    some_function1, some_function2, some_function3, some_function4, some_polymorphic_function2,
    Variant,
};

use like::{Escape, Like};
use regex::Regex;

#[doc(hidden)]
pub fn concat_s_s(mut left: String, right: String) -> String {
    left.reserve(right.len());
    left.push_str(&right);
    left
}

some_polymorphic_function2!(concat, s, String, s, String, String);

#[doc(hidden)]
pub fn substring3___(value: String, left: i32, count: i32) -> String {
    if count < 0 {
        String::new()
    } else {
        // character indexes in SQL start at 1
        let start = if left < 1 { 0 } else { left - 1 };
        value
            .chars()
            .skip(start as usize)
            .take(count as usize)
            .collect()
    }
}

some_function3!(substring3, String, i32, i32, String);

#[doc(hidden)]
pub fn substring2__(value: String, left: i32) -> String {
    // character indexes in SQL start at 1
    let start = if left < 1 { 0 } else { left - 1 };
    value.chars().skip(start as usize).collect()
}

some_function2!(substring2, String, i32, String);

#[doc(hidden)]
pub fn trim_both_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_matches(chr).to_string()
}

some_polymorphic_function2!(trim_both, s, String, s, String, String);

#[doc(hidden)]
pub fn trim_leading_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_start_matches(chr).to_string()
}

some_polymorphic_function2!(trim_leading, s, String, s, String, String);

#[doc(hidden)]
pub fn trim_trailing_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_end_matches(chr).to_string()
}

some_polymorphic_function2!(trim_trailing, s, String, s, String, String);

#[doc(hidden)]
pub fn like2__(value: String, pattern: String) -> bool {
    Like::<false>::like(value.as_str(), pattern.as_str()).unwrap()
}

some_function2!(like2, String, String, bool);

#[doc(hidden)]
pub fn rlike__(value: String, pattern: String) -> bool {
    let re = Regex::new(&pattern).ok();
    rlikeC__(value, &re)
}

// rlike with a Constant regular expression.
// re is None when the regular expression expression is malformed,
// In this case the result is false and not None.
// The regular expression cannot be null - the compiler would detect that.
#[doc(hidden)]
pub fn rlikeC__(value: String, re: &Option<Regex>) -> bool {
    match re {
        None => false,
        Some(re) => re.is_match(&value),
    }
}

#[doc(hidden)]
pub fn rlikeCN_(value: Option<String>, re: &Option<Regex>) -> Option<bool> {
    let value = value?;
    Some(rlikeC__(value, re))
}

some_function2!(rlike, String, String, bool);

#[doc(hidden)]
pub fn like3___(value: String, pattern: String, escape: String) -> bool {
    let escaped = pattern.as_str().escape(escape.as_str()).unwrap();
    Like::<true>::like(value.as_str(), escaped.as_str()).unwrap()
}

some_function3!(like3, String, String, String, bool);

#[doc(hidden)]
pub fn ilike2__(value: String, pattern: String) -> bool {
    // Convert both the value and the pattern to lowercase for case-insensitive comparison
    Like::<false>::like(
        value.to_lowercase().as_str(),
        pattern.to_lowercase().as_str(),
    )
    .unwrap()
}

some_function2!(ilike2, String, String, bool);

#[doc(hidden)]
pub fn position__(needle: String, haystack: String) -> i32 {
    let pos = haystack.find(needle.as_str());
    match pos {
        None => 0,
        Some(i) => (i + 1) as i32,
    }
}

some_function2!(position, String, String, i32);

#[doc(hidden)]
pub fn char_length_(value: String) -> i32 {
    value.chars().count() as i32
}

some_function1!(char_length, String, i32);

#[doc(hidden)]
pub fn char_length_ref(value: &str) -> i32 {
    value.chars().count() as i32
}

#[doc(hidden)]
pub fn ascii_(value: String) -> i32 {
    if value.is_empty() {
        0
    } else {
        value.chars().next().unwrap() as u32 as i32
    }
}

some_function1!(ascii, String, i32);

#[doc(hidden)]
pub fn chr_(code: i32) -> String {
    if code < 0 {
        String::default()
    } else {
        let c = char::from_u32(code as u32);
        match c {
            None => String::default(),
            Some(v) => String::from(v),
        }
    }
}

some_function1!(chr, i32, String);

#[doc(hidden)]
pub fn repeat__(value: String, count: i32) -> String {
    if count <= 0 {
        String::default()
    } else {
        value.repeat(count as usize)
    }
}

some_function2!(repeat, String, i32, String);

#[doc(hidden)]
pub fn overlay3___(source: String, replacement: String, position: i32) -> String {
    let len = char_length_ref(&replacement);
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, String, String, i32, String);

#[doc(hidden)]
pub fn overlay4____(source: String, replacement: String, position: i32, remove: i32) -> String {
    let mut remove = remove;
    if remove < 0 {
        remove = 0;
    }
    if position <= 0 {
        source
    } else if position > char_length_ref(&source) {
        concat_s_s(source, replacement)
    } else {
        let mut result = substring3___(source.clone(), 0, position - 1);
        result += &replacement;
        result += &substring2__(source, position + remove);
        result
    }
}

some_function4!(overlay4, String, String, i32, i32, String);

#[doc(hidden)]
pub fn lower_(source: String) -> String {
    source.to_lowercase()
}

some_function1!(lower, String, String);

#[doc(hidden)]
pub fn upper_(source: String) -> String {
    source.to_uppercase()
}

some_function1!(upper, String, String);

#[doc(hidden)]
pub fn initcap_(source: String) -> String {
    let mut result = String::with_capacity(source.len());
    let mut capitalize_next = true;
    for c in source.chars() {
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
    result
}

some_function1!(initcap, String, String);

#[doc(hidden)]
pub fn replace___(haystack: String, needle: String, replacement: String) -> String {
    haystack.replace(&needle, &replacement)
}

some_function3!(replace, String, String, String, String);

#[doc(hidden)]
pub fn left__(source: String, size: i32) -> String {
    substring3___(source, 1, size)
}

some_function2!(left, String, i32, String);

#[doc(hidden)]
pub fn split2__(source: String, separators: String) -> Vec<String> {
    if separators.is_empty() {
        return vec![source];
    }
    if source.is_empty() {
        return vec![];
    }
    source.split(&separators).map(String::from).collect()
}

some_function2!(split2, String, String, Vec<String>);

#[doc(hidden)]
pub fn split1_(source: String) -> Vec<String> {
    split2__(source, ",".to_string())
}

some_function1!(split1, String, Vec<String>);

#[doc(hidden)]
pub fn split_part___(s: String, delimiter: String, n: i32) -> String {
    let parts: Vec<String> = split2__(s, delimiter);
    let part_count = parts.len() as i32;

    // Handle negative indices
    let n = if n < 0 { part_count + n + 1 } else { n };

    if n <= 0 || n > part_count {
        return String::new();
    }

    parts[(n - 1) as usize].to_string()
}

some_function3!(split_part, String, String, i32, String);

#[doc(hidden)]
pub fn array_to_string2_vec__(value: Vec<String>, separator: String) -> String {
    value.join(&separator)
}

some_function2!(array_to_string2_vec, Vec<String>, String, String);

#[doc(hidden)]
pub fn array_to_string2Nvec__(value: Vec<Option<String>>, separator: String) -> String {
    let capacity = value
        .iter()
        .map(|s| s.as_ref().map_or(0, |s| s.len()))
        .sum();
    let mut result = String::with_capacity(capacity);
    let mut first = true;
    for word in value {
        let append = match word.as_ref() {
            None => {
                continue;
            }
            Some(r) => r,
        };
        if !first {
            result.push_str(&separator)
        }
        first = false;
        result.push_str(append.as_str());
    }
    result
}

some_function2!(array_to_string2Nvec, Vec<Option<String>>, String, String);

#[doc(hidden)]
pub fn array_to_string3_vec___(
    value: Vec<String>,
    separator: String,
    _null_value: String,
) -> String {
    array_to_string2_vec__(value, separator)
}

some_function3!(array_to_string3_vec, Vec<String>, String, String, String);

#[doc(hidden)]
pub fn array_to_string3Nvec___(
    value: Vec<Option<String>>,
    separator: String,
    null_value: String,
) -> String {
    let null_size = null_value.len();
    let capacity = value
        .iter()
        .map(|s| s.as_ref().map_or(null_size, |s| s.len()))
        .sum();
    let mut result = String::with_capacity(capacity);
    let mut first = true;
    for word in value {
        let append = match word.as_ref() {
            None => null_value.as_str(),
            Some(r) => r,
        };
        if !first {
            result.push_str(&separator)
        }
        first = false;
        result.push_str(append);
    }
    result
}

some_function3!(
    array_to_string3Nvec,
    Vec<Option<String>>,
    String,
    String,
    String
);

#[doc(hidden)]
pub fn writelog<T: std::fmt::Display>(format: String, argument: T) -> T {
    let format_arg = format!("{}", argument);
    let formatted = format.replace("%%", &format_arg);
    print!("{}", formatted);
    argument
}

#[doc(hidden)]
pub fn parse_json_(value: String) -> Variant {
    match serde_json::from_str::<Variant>(&value) {
        Ok(v) => v,
        Err(_) => Variant::SqlNull,
    }
}

some_function1!(parse_json, String, Variant);

#[doc(hidden)]
pub fn to_json_V(value: Variant) -> Option<String> {
    match value.to_json_string() {
        Ok(s) => Some(s),
        _ => None,
    }
}

#[doc(hidden)]
pub fn to_json_VN(value: Option<Variant>) -> Option<String> {
    let value = value?;
    to_json_V(value)
}

#[doc(hidden)]
pub fn to_json_nullN(_value: Option<()>) -> Option<String> {
    None
}

#[doc(hidden)]
pub fn regexp_replace3___(str: String, re: String, repl: String) -> String {
    let re = Regex::new(&re).ok();
    regexp_replaceC3___(str, &re, repl)
}

some_function3!(regexp_replace3, String, String, String, String);

#[doc(hidden)]
pub fn regexp_replace2__(str: String, re: String) -> String {
    regexp_replace3___(str, re, "".to_string())
}

some_function2!(regexp_replace2, String, String, String);

#[doc(hidden)]
pub fn regexp_replaceC3___(str: String, re: &Option<Regex>, repl: String) -> String {
    match re {
        None => str,
        Some(re) => re.replace_all(&str, repl).to_string(),
    }
}

#[doc(hidden)]
pub fn regexp_replaceC3N__(
    str: Option<String>,
    re: &Option<Regex>,
    repl: String,
) -> Option<String> {
    let str = str?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC3__N(
    str: String,
    re: &Option<Regex>,
    repl: Option<String>,
) -> Option<String> {
    let repl = repl?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC3N_N(
    str: Option<String>,
    re: &Option<Regex>,
    repl: Option<String>,
) -> Option<String> {
    let str = str?;
    let repl = repl?;
    Some(regexp_replaceC3___(str, re, repl))
}

#[doc(hidden)]
pub fn regexp_replaceC2__(str: String, re: &Option<Regex>) -> String {
    regexp_replaceC3___(str, re, "".to_string())
}

#[doc(hidden)]
pub fn regexp_replaceC2N_(str: Option<String>, re: &Option<Regex>) -> Option<String> {
    let str = str?;
    Some(regexp_replaceC3___(str, re, "".to_string()))
}

#[doc(hidden)]
pub fn concat_ws___(sep: String, mut left: String, right: String) -> String {
    left.reserve(right.len() + sep.len());
    left.push_str(&sep);
    left.push_str(&right);
    left
}

#[doc(hidden)]
pub fn concat_wsN__(sep: Option<String>, left: String, right: String) -> Option<String> {
    let sep = sep?;
    Some(concat_ws___(sep, left, right))
}

#[doc(hidden)]
pub fn concat_ws_N_(sep: String, left: Option<String>, right: String) -> String {
    match left {
        None => right,
        Some(left) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_ws_NN(sep: String, left: Option<String>, right: Option<String>) -> String {
    match (left, right) {
        (None, None) => "".to_string(),
        (None, Some(right)) => right,
        (Some(left), None) => left,
        (Some(left), Some(right)) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_wsNN_(sep: Option<String>, left: Option<String>, right: String) -> Option<String> {
    let sep = sep?;
    Some(concat_ws_N_(sep, left, right))
}

#[doc(hidden)]
pub fn concat_ws__N(sep: String, left: String, right: Option<String>) -> String {
    match right {
        None => left,
        Some(right) => concat_ws___(sep, left, right),
    }
}

#[doc(hidden)]
pub fn concat_wsN_N(sep: Option<String>, left: String, right: Option<String>) -> Option<String> {
    let sep = sep?;
    Some(concat_ws__N(sep, left, right))
}

#[doc(hidden)]
pub fn concat_wsNNN(
    sep: Option<String>,
    left: Option<String>,
    right: Option<String>,
) -> Option<String> {
    let sep = sep?;
    Some(concat_ws_NN(sep, left, right))
}
