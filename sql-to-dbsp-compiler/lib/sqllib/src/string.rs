//! SQL String operations

#![allow(non_snake_case)]

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

pub fn substring___3(value: String, left: i32, count: i32) -> String {
    if count < 0 { return "".to_string() }
    else {
        // character indexes in SQL start at 1
        let start = if left < 1 { 0 } else { left - 1 };
        value.chars().skip(start as usize).take(count as usize).collect()
    }
}

pub fn substringNNN3(value: Option<String>, left: Option<i32>, count: Option<i32>) -> Option<String> {
    let value = value?;
    let left = left?;
    let count = count?;
    Some(substring___3(value, left, count))
}

pub fn substringNN_3(value: Option<String>, left: Option<i32>, count: i32) -> Option<String> {
    let value = value?;
    let left = left?;
    Some(substring___3(value, left, count))
}

pub fn substringN_N3(value: Option<String>, left: i32, count: Option<i32>) -> Option<String> {
    let value = value?;
    let count = count?;
    Some(substring___3(value, left, count))
}

pub fn substring_NN3(value: String, left: Option<i32>, count: Option<i32>) -> Option<String> {
    let left = left?;
    let count = count?;
    Some(substring___3(value, left, count))
}

pub fn substring__N3(value: String, left: i32, count: Option<i32>) -> Option<String> {
    count.map(|c| substring___3(value, left, c))
}

pub fn substring_N_3(value: String, left: Option<i32>, count: i32) -> Option<String> {
    left.map(|l| substring___3(value, l, count))
}

pub fn substringN__3(value: Option<String>, left: i32, count: i32) -> Option<String> {
    value.map(|value| substring___3(value, left, count))
}

pub fn substring__2(value: String, left: i32) -> String {
    // character indexes in SQL start at 1
    let start = if left < 1 { 0 } else { left - 1 };
    value.chars().skip(start as usize).collect()
}

pub fn substringNN2(value: Option<String>, left: Option<i32>) -> Option<String> {
    let value = value?;
    let left = left?;
    Some(substring__2(value, left))
}

pub fn substringN_2(value: Option<String>, left: i32) -> Option<String> {
    value.map(|v| substring__2(v, left))
}

pub fn substring_N2(value: String, left: Option<i32>) -> Option<String> {
    left.map(|l| substring__2(value, l))
}

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

pub fn like__2(value: String, pattern: String) -> bool {
    Like::<false>::like(value.as_str(), pattern.as_str()).unwrap()
}

pub fn like_N2(value: String, pattern: Option<String>) -> Option<bool> {
    pattern.map(|v| like__2(value, v))
}

pub fn likeN_2(value: Option<String>, pattern: String) -> Option<bool> {
    value.map(|v| like__2(v, pattern))
}

pub fn likeNN2(value: Option<String>, pattern: Option<String>) -> Option<bool> {
    let value = value?;
    let pattern = pattern?;
    Some(like__2(value, pattern))
}

pub fn like___3(value: String, pattern: String, escape: String) -> bool {
    let escaped = pattern.as_str().escape(escape.as_str()).unwrap();
    Like::<true>::like(value.as_str(), escaped.as_str()).unwrap()
}

pub fn likeN__3(value: Option<String>, pattern: String, escape: String) -> Option<bool> {
    value.map(|value| like___3(value, pattern, escape))
}

pub fn like_N_3(value: String, pattern: Option<String>, escape: String) -> Option<bool> {
    pattern.map(|pattern| like___3(value, pattern, escape))
}

pub fn like__N3(value: String, pattern: String, escape: Option<String>) -> Option<bool> {
    escape.map(|escape| like___3(value, pattern, escape))
}

pub fn likeNN_3(value: Option<String>, pattern: Option<String>, escape: String) -> Option<bool> {
    let value = value?;
    let pattern = pattern?;
    Some(like___3(value, pattern, escape))
}

pub fn likeN_N3(value: Option<String>, pattern: String, escape: Option<String>) -> Option<bool> {
    let value = value?;
    let escape = escape?;
    Some(like___3(value, pattern, escape))
}

pub fn like_NN3(value: String, pattern: Option<String>, escape: Option<String>) -> Option<bool> {
    let pattern = pattern?;
    let escape = escape?;
    Some(like___3(value, pattern, escape))
}

pub fn likeNNN3(value: Option<String>, pattern: Option<String>, escape: Option<String>) -> Option<bool> {
    let value = value?;
    let pattern = pattern?;
    let escape = escape?;
    Some(like___3(value, pattern, escape))
}

pub fn position__(needle: String, haystack: String) -> i32 {
    let pos = haystack.find(needle.as_str());
    match pos {
        None => 0,
        Some(i) => (i + 1) as i32,
    }
}

pub fn positionN_(needle: Option<String>, haystack: String) -> Option<i32> {
    needle.map(|n| position__(n, haystack))
}

pub fn position_N(needle: String, haystack: Option<String>) -> Option<i32> {
    haystack.map(|h| position__(needle, h))
}

pub fn positionNN(needle: Option<String>, haystack: Option<String>) -> Option<i32> {
    let needle = needle?;
    let haystack = haystack?;
    Some(position__(needle, haystack))
}

pub fn char_length_(value: String) -> i32 {
    value.chars().count() as i32
}

pub fn char_lengthN(value: Option<String>) -> Option<i32> {
    value.map(|v| char_length_(v))
}

pub fn char_length_ref(value: &String) -> i32 {
    value.chars().count() as i32
}

pub fn ascii_(value: String) -> i32 {
    if value.is_empty() { 0 }
    else { value.chars().next().unwrap() as u32 as i32 }
}

pub fn asciiN(value: Option<String>) -> Option<i32> {
    value.map(|v| ascii_(v))
}

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

pub fn chrN(code: Option<i32>) -> Option<String> {
    code.map(|x| chr_(x))
}

pub fn repeat__(value: String, count: i32) -> String {
    if count <= 0 { String::default() }
    else { value.repeat(count as usize) }
}

pub fn repeat_N(value: String, count: Option<i32>) -> Option<String> {
    count.map(|c| repeat__(value, c))
}

pub fn repeatN_(value: Option<String>, count: i32) -> Option<String> {
    value.map(|v| repeat__(v, count))
}

pub fn repeatNN(value: Option<String>, count: Option<i32>) -> Option<String> {
    let value = value?;
    let count = count?;
    Some(repeat__(value, count))
}

pub fn overlay___3(source: String, replacement: String, position: i32) -> String {
    let len = char_length_ref(&replacement);
    overlay____4(source, replacement, position, len)
}

pub fn overlayNNN3(source: Option<String>, replacement: Option<String>, position: Option<i32>) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    let position = position?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlayNN_3(source: Option<String>, replacement: Option<String>, position: i32) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlayN_N3(source: Option<String>, replacement: String, position: Option<i32>) -> Option<String> {
    let source = source?;
    let position = position?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlay_NN3(source: String, replacement: Option<String>, position: Option<i32>) -> Option<String> {
    let replacement = replacement?;
    let position = position?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlay__N3(source: String, replacement: String, position: Option<i32>) -> Option<String> {
    let position = position?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlay_N_3(source: String, replacement: Option<String>, position: i32) -> Option<String> {
    let replacement = replacement?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlayN__3(source: Option<String>, replacement: String, position: i32) -> Option<String> {
    let source = source?;
    Some(overlay___3(source, replacement, position))
}

pub fn overlay____4(source: String, replacement: String, position: i32, remove: i32) -> String {
    let mut remove = remove;
    if remove < 0 { remove = 0; }
    if position <= 0 { source }
    else if position > char_length_ref(&source) { concat_s_s(source, replacement) }
    else {
        let mut result = substring___3(source.clone(), 0, position - 1);
        result += &replacement;
        result += &substring__2(source, position + remove);
        result
    }
}

pub fn overlay___N4(source: String, replacement: String, position: i32, remove: Option<i32>) -> Option<String> {
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay__N_4(source: String, replacement: String, position: Option<i32>, remove: i32) -> Option<String> {
    let position = position?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay__NN4(source: String, replacement: String, position: Option<i32>, remove: Option<i32>) -> Option<String> {
    let position = position?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay_N__4(source: String, replacement: Option<String>, position: i32, remove: i32) -> Option<String> {
    let replacement = replacement?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay_N_N4(source: String, replacement: Option<String>, position: i32, remove: Option<i32>) -> Option<String> {
    let replacement = replacement?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay_NN_4(source: String, replacement: Option<String>, position: Option<i32>, remove: i32) -> Option<String> {
    let replacement = replacement?;
    let position = position?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlay_NNN4(source: String, replacement: Option<String>, position: Option<i32>, remove: Option<i32>) -> Option<String> {
    let replacement = replacement?;
    let position = position?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayN___4(source: Option<String>, replacement: String, position: i32, remove: i32) -> Option<String> {
    let source = source?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayN__N4(source: Option<String>, replacement: String, position: i32, remove: Option<i32>) -> Option<String> {
    let source = source?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayN_N_4(source: Option<String>, replacement: String, position: Option<i32>, remove: i32) -> Option<String> {
    let source = source?;
    let position = position?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayN_NN4(source: Option<String>, replacement: String, position: Option<i32>, remove: Option<i32>) -> Option<String> {
    let source = source?;
    let position = position?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayNN__4(source: Option<String>, replacement: Option<String>, position: i32, remove: i32) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayNN_N4(source: Option<String>, replacement: Option<String>, position: i32, remove: Option<i32>) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayNNN_4(source: Option<String>, replacement: Option<String>, position: Option<i32>, remove: i32) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    let position = position?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn overlayNNNN4(source: Option<String>, replacement: Option<String>, position: Option<i32>, remove: Option<i32>) -> Option<String> {
    let source = source?;
    let replacement = replacement?;
    let position = position?;
    let remove = remove?;
    Some(overlay____4(source, replacement, position, remove))
}

pub fn lower_(source: String) -> String {
    source.to_lowercase()
}

pub fn lowerN(source: Option<String>) -> Option<String> {
    source.map(|x| lower_(x))
}

pub fn upper_(source: String) -> String {
    source.to_uppercase()
}

pub fn upperN(source: Option<String>) -> Option<String> {
    source.map(|x| upper_(x))
}
