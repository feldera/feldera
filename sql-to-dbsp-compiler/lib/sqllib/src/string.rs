//! SQL String operations

#![allow(non_snake_case)]
use crate::{
    some_function1, some_function2, some_function3, some_function4, some_polymorphic_function2,
};

use like::{Escape, Like};
use regex::Regex;

pub fn concat_s_s(mut left: String, right: String) -> String {
    left.reserve(right.len());
    left.push_str(&right);
    left
}

some_polymorphic_function2!(concat, s, String, s, String, String);

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

pub fn substring2__(value: String, left: i32) -> String {
    // character indexes in SQL start at 1
    let start = if left < 1 { 0 } else { left - 1 };
    value.chars().skip(start as usize).collect()
}

some_function2!(substring2, String, i32, String);

pub fn trim_both_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_matches(chr).to_string()
}

some_polymorphic_function2!(trim_both, s, String, s, String, String);

pub fn trim_leading_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_start_matches(chr).to_string()
}

some_polymorphic_function2!(trim_leading, s, String, s, String, String);

pub fn trim_trailing_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_end_matches(chr).to_string()
}

some_polymorphic_function2!(trim_trailing, s, String, s, String, String);

pub fn like2__(value: String, pattern: String) -> bool {
    Like::<false>::like(value.as_str(), pattern.as_str()).unwrap()
}

some_function2!(like2, String, String, bool);

pub fn rlike__(value: String, pattern: String) -> bool {
    let re = Regex::new(&pattern);
    re.map_or_else(|_| false, |re| re.is_match(&value))
}

some_function2!(rlike, String, String, bool);

pub fn like3___(value: String, pattern: String, escape: String) -> bool {
    let escaped = pattern.as_str().escape(escape.as_str()).unwrap();
    Like::<true>::like(value.as_str(), escaped.as_str()).unwrap()
}

some_function3!(like3, String, String, String, bool);

pub fn position__(needle: String, haystack: String) -> i32 {
    let pos = haystack.find(needle.as_str());
    match pos {
        None => 0,
        Some(i) => (i + 1) as i32,
    }
}

some_function2!(position, String, String, i32);

pub fn char_length_(value: String) -> i32 {
    value.chars().count() as i32
}

some_function1!(char_length, String, i32);

pub fn char_length_ref(value: &str) -> i32 {
    value.chars().count() as i32
}

pub fn ascii_(value: String) -> i32 {
    if value.is_empty() {
        0
    } else {
        value.chars().next().unwrap() as u32 as i32
    }
}

some_function1!(ascii, String, i32);

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

pub fn repeat__(value: String, count: i32) -> String {
    if count <= 0 {
        String::default()
    } else {
        value.repeat(count as usize)
    }
}

some_function2!(repeat, String, i32, String);

pub fn overlay3___(source: String, replacement: String, position: i32) -> String {
    let len = char_length_ref(&replacement);
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, String, String, i32, String);

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

pub fn lower_(source: String) -> String {
    source.to_lowercase()
}

some_function1!(lower, String, String);

pub fn upper_(source: String) -> String {
    source.to_uppercase()
}

some_function1!(upper, String, String);

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

pub fn replace___(haystack: String, needle: String, replacement: String) -> String {
    haystack.replace(&needle, &replacement)
}

some_function3!(replace, String, String, String, String);

pub fn left__(source: String, size: i32) -> String {
    substring3___(source, 1, size)
}

some_function2!(left, String, i32, String);

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

pub fn split1_(source: String) -> Vec<String> {
    split2__(source, ",".to_string())
}

some_function1!(split1, String, Vec<String>);

pub fn array_to_string2_vec__(value: Vec<String>, separator: String) -> String {
    value.join(&separator)
}

some_function2!(array_to_string2_vec, Vec<String>, String, String);

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

pub fn array_to_string3_vec___(
    value: Vec<String>,
    separator: String,
    _null_value: String,
) -> String {
    array_to_string2_vec__(value, separator)
}

some_function3!(array_to_string3_vec, Vec<String>, String, String, String);

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

pub fn writelog<T: std::fmt::Display>(format: String, argument: T) -> T {
    let format_arg = format!("{}", argument);
    let formatted = format.replace("%%", &format_arg);
    print!("{}", formatted);
    argument
}
