//! SQL String operations

#![allow(non_snake_case)]
use crate::{
    some_function1, some_function2, some_function3, some_function4, some_polymorphic_function2,
};

use like::{Escape, Like};
use regex::Regex;

use md5::{Md5, Digest};

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
    let letters: Vec<char> = remove.chars().collect();
    value.trim_matches(&letters as &[_]).to_string()
}

some_polymorphic_function2!(trim_both, s, String, s, String, String);

pub fn trim_leading_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let letters: Vec<char> = remove.chars().collect();
    value.trim_start_matches(&letters as &[_]).to_string()
}

some_polymorphic_function2!(trim_leading, s, String, s, String, String);

pub fn trim_trailing_s_s(remove: String, value: String) -> String {
    // 'remove' always has exactly 1 character
    let letters: Vec<char> = remove.chars().collect();
    value.trim_end_matches(&letters as &[_]).to_string()
}

some_polymorphic_function2!(trim_trailing, s, String, s, String, String);

pub fn like2__(value: String, pattern: String) -> bool {
    Like::<false>::like(value.as_str(), pattern.as_str()).unwrap()
}

pub fn like2N_(value: Option<String>, pattern: String) -> bool {
    match value {
        None => false,
        Some(value) => like2__(value, pattern),
    }
}

pub fn rlike__(value: String, pattern: String) -> bool {
    // TODO: the regex should not be created for each row.
    let re = Regex::new(&pattern);
    re.map_or_else(|_| false, |re| re.is_match(&value))
}

pub fn rlikeN_(value: Option<String>, pattern: String) -> bool {
    match value {
        None => false,
        Some(value) => rlike__(value, pattern),
    }
}

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

pub fn md5__(value: String) -> String{
    let mut  hasher = Md5::new();
    hasher.input(value.as_bytes());
    let result = hasher.result();
    return format!("{:x}", result);
}

pub fn to_lower_hex(value: i64) -> String{
    format!("{:x}", value)
}

pub fn to_upper_hex(value: i64) -> String{
    format!("{:X}", value)
}

pub fn translate_(value: String, from: String, to: String) -> String{
    let mut  index =   0;
    let len_of_to = to.len();
    let mut tmp_value =  (&value).to_string();
    for x in from.chars() {
        if index < len_of_to{
            let inn = index.clone();
            let char_replace = to.chars().nth(inn).unwrap().to_string();
            tmp_value = tmp_value.replace(x, &*(char_replace));
        }else{
            tmp_value = tmp_value.replace(x, "");
        }
        index+=1;
    }
    tmp_value
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_concat_s_s() {
        let result = concat_s_s("Post".to_string(), "greSQL".to_string());
        assert_eq!(result, "PostgreSQL".to_string());
    }

    #[test]
    fn test_substring3___(){
        assert_eq!(substring3___("Thomas".to_string(), 2, 3), "hom".to_string());
        let text = &mut "Thomas".to_string();
        assert_eq!(substring3___(text.to_string(), 3, text.len() as i32), "omas".to_string());
    }

    #[test]
    fn test_substring2__(){
        assert_eq!(substring2__("Thomas".to_string(), 2), "homas".to_string());
        assert_eq!(substring2__("Thomas".to_string(), 3), "omas".to_string());
    }

    #[test]
    fn test_trim_both_s_s(){
        assert_eq!(trim_both_s_s( "xyz".to_string(), "xyxTomxx".to_string() ), "Tom".to_string());
    }

    #[test]
    fn test_trim_leading_s_s(){
        assert_eq!(trim_leading_s_s("yx".to_string(), "yxTomxx".to_string()), "Tomxx".to_string());
    }

    #[test]
    fn test_trim_trailing_s_s(){
        assert_eq!(trim_trailing_s_s("xx".to_string(), "yxxTomxx".to_string()), "yxxTom".to_string());
    }

    #[test]
    fn test_like2__(){
        assert_eq!(like2__("Tomxx".to_string(), "%Tom%".to_string()), true);
        assert_eq!(like2__("Tomxx".to_string(), "Tom%".to_string()), true);
        assert_eq!(like2__("Tomxx".to_string(), "Toz".to_string()), false);
    }

    #[test]
    fn test_like2N_(){
        assert_eq!(like2N_(Some("Tomxx".to_string()), "%Tom%".to_string()), true);
        assert_eq!(like2N_(None, "Tom%".to_string()), false);
        assert_eq!(like2N_(None, "".to_string()), false);
    }

    #[test]
    fn test_rlike__(){
        assert_eq!(rlike__("Tomxx".to_string(), "Tom".to_string()), true);
    }

    #[test]
    fn test_rlikeN_(){
        assert_eq!(rlikeN_(Some("Tomxx".to_string()), "Tom".to_string()), true);
        assert_eq!(rlikeN_(None, "Tom".to_string()), false);
    }

    #[test]
    fn test_position__(){
        assert_eq!(position__("Tom".to_string(), "Tomxxx".to_string()), 1);
        assert_eq!(position__("Tom".to_string(), "testTomxxx".to_string()), 5);
    }
    #[test]
    fn test_char_length_(){
        assert_eq!(char_length_("Tom".to_string()), 3);
    }

    #[test]
    fn test_char_length_ref(){
        assert_eq!(char_length_ref(&"Tom"), 3);
    }

    #[test]
    fn test_ascii_(){
        assert_eq!(ascii_("123".to_string()), 49);
        assert_eq!(ascii_("".to_string()), 0);
    }

    #[test]
    fn test_chr_(){
        assert_eq!(chr_(65), "A");
    }

    #[test]
    fn test_repeat__(){
        assert_eq!(repeat__("Postgre".to_string(), 4), "PostgrePostgrePostgrePostgre".to_string());
        assert_eq!(repeat__("Postgre".to_string(), 0), "".to_string());
    }

    #[test]
    fn test_overlay3___(){
        assert_eq!(overlay3___("Txxxxas".to_string(), "hom".to_string(), 2), "Thomxas".to_string());
    }

    #[test]
    fn test_overlay4___(){
        assert_eq!(overlay4____("Txxxxas".to_string(), "hom".to_string(), 2, 4), "Thomas".to_string());
    }

    #[test]
    fn test_lower_(){
        assert_eq!(lower_("Thomas".to_string()), "thomas".to_string());
    }

    #[test]
    fn test_upper_(){
        assert_eq!(upper_("Thomas".to_string()), "THOMAS".to_string());
    }

    #[test]
    fn test_initcap_(){
        assert_eq!(initcap_("hi THOMAS".to_string()), "Hi Thomas".to_string());
    }

    #[test]
    fn test_replace___(){
        assert_eq!(replace___("hi Thomas".to_string(), "hi".to_string(), "Hello".to_string()), "Hello Thomas".to_string());
    }

    #[test]
    fn test_left__(){
        assert_eq!(left__("hi Thomas".to_string(), 2), "hi".to_string());
    }

    #[test]
    fn test_split2__(){
        assert_eq!(split2__("hi Thomas".to_string(), " ".to_string()), vec!["hi".to_string(), "Thomas".to_string()]);
    }

    #[test]
    fn test_split1_(){
        assert_eq!(split1_("hi,Thomas".to_string()), vec!["hi".to_string(), "Thomas".to_string()]);
    }

    #[test]
    fn test_array_to_string2_vec__(){
        assert_eq!(array_to_string2_vec__(vec!["John".to_string(), "Thomas".to_string()], ",".to_string()), "John,Thomas".to_string());
    }

    #[test]
    fn test_array_to_string2Nvec__(){
        assert_eq!(array_to_string2Nvec__(vec![Some("John".to_string()), None, Some("Thomas".to_string())], " ".to_string()), "John Thomas".to_string());
    }

    #[test]
    fn test_array_to_string3_vec___(){
        assert_eq!(array_to_string3_vec___(vec!["John".to_string(), "Thomas".to_string()], " ".to_string(),"".to_string()), "John Thomas".to_string());
    }

    #[test]
    fn test_array_to_string3Nvec___(){
        assert_eq!(array_to_string3Nvec___(vec![Some("John".to_string()), None, Some("Thomas".to_string())], " ".to_string(),"None".to_string()), "John None Thomas".to_string());
    }

    #[test]
    fn test_to_lower_hex(){
        assert_eq!(to_lower_hex(2147483647), "7fffffff");
    }

    #[test]
    fn test_to_upper_hex(){
        assert_eq!(to_upper_hex(2147483647), "7FFFFFFF");
    }

    #[test]
    fn test_md5__(){
        assert_eq!(md5__("Value".to_string()), "689202409e48743b914713f96d93947c".to_string());
    }

    #[test]
    fn test_translate_(){
        assert_eq!(translate_("12345".to_string(), "143".to_string(), "ax".to_string()), "a2x5".to_string());
    }
}
