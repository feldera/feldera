//! SQL String operations

#![allow(non_snake_case)]
use crate::some_function1;
use crate::some_function2;
use crate::some_function3;
use crate::some_function4;

use like::{Like, Escape};

pub fn concat_s_s(left: String, right: String) -> String {
    let result = format!("{}{}", left, right);
    result
}

pub fn concat_sN_s(left: Option<String>, right: String) -> Option<String> {
    left.map(|v| concat_s_s(v, right))
}

pub fn concat_s_sN(left: String, right: Option<String>) -> Option<String> {
    right.map(|v| concat_s_s(left, v))
}

pub fn concat_sN_sN(left: Option<String>, right: Option<String>) -> Option<String> {
    let left = left?;
    let right = right?;
    Some(concat_s_s(left, right))
}

pub fn substring3___(value: String, left: i32, count: i32) -> String {
    if count < 0 { return "".to_string() }
    else {
        // character indexes in SQL start at 1
        let start = if left < 1 { 0 } else { left - 1 };
        value.chars().skip(start as usize).take(count as usize).collect()
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

pub fn trim_leading_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_start_matches(chr).to_string()
}

pub fn trim_trailing_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let chr = remove.chars().next().unwrap();
    value.trim_end_matches(chr).to_string()
}

pub fn like2__(value: String, pattern: String) -> bool {
    Like::<false>::like(value.as_str(), pattern.as_str()).unwrap()
}

some_function2!(like2, String, String, bool);

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

pub fn char_length_ref(value: &String) -> i32 {
    value.chars().count() as i32
}

pub fn ascii_(value: String) -> i32 {
    if value.is_empty() { 0 }
    else { value.chars().next().unwrap() as u32 as i32 }
}

some_function1!(ascii, String, i32);

pub fn chr_(code: i32) -> String {
    if code < 0 { String::default() }
    else {
        let c = char::from_u32(code as u32);
        match c {
            None => String::default(),
            Some(v) => String::from(v),
        }
    }
}

some_function1!(chr, i32, String);

pub fn repeat__(value: String, count: i32) -> String {
    if count <= 0 { String::default() }
    else { value.repeat(count as usize) }
}

some_function2!(repeat, String, i32, String);

pub fn overlay3___(source: String, replacement: String, position: i32) -> String {
    let len = char_length_ref(&replacement);
    overlay4____(source, replacement, position, len)
}

some_function3!(overlay3, String, String, i32, String);

pub fn overlay4____(source: String, replacement: String, position: i32, remove: i32) -> String {
    let mut remove = remove;
    if remove < 0 { remove = 0; }
    if position <= 0 { source }
    else if position > char_length_ref(&source) { concat_s_s(source, replacement) }
    else {
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
