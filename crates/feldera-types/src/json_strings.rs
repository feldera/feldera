//! Rewriting of the strings of a JSON value.

use serde_json::Value;
use std::borrow::Cow;

/// Passes every string of `value` through `rewrite`, object keys included.
/// `rewrite` returns [`Cow::Borrowed`] for a string it leaves as it is.
///
/// Two keys can be rewritten to the same name. The second one then gets a
/// suffix, ` (2)`, so that no field is lost.
pub fn rewrite_json_strings(value: &mut Value, rewrite: &dyn Fn(&str) -> Cow<'_, str>) {
    match value {
        Value::String(text) => {
            if let Cow::Owned(rewritten) = rewrite(text) {
                *text = rewritten;
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| rewrite_json_strings(item, rewrite)),
        Value::Object(fields) => {
            let mut renamed = Vec::new();
            for (key, field) in fields.iter_mut() {
                rewrite_json_strings(field, rewrite);
                if let Cow::Owned(rewritten) = rewrite(key) {
                    renamed.push((key.clone(), rewritten));
                }
            }
            for (key, rewritten) in renamed {
                let Some(field) = fields.remove(&key) else {
                    continue;
                };
                let mut name = rewritten.clone();
                let mut n = 1;
                while fields.contains_key(&name) {
                    n += 1;
                    name = format!("{rewritten} ({n})");
                }
                fields.insert(name, field);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_json_strings;
    use serde_json::json;
    use std::borrow::Cow;

    fn hide_digits(text: &str) -> Cow<'_, str> {
        if text.contains(|c: char| c.is_ascii_digit()) {
            Cow::Owned(text.replace(|c: char| c.is_ascii_digit(), "#"))
        } else {
            Cow::Borrowed(text)
        }
    }

    #[test]
    fn strings_and_keys_are_rewritten() {
        let mut value = json!({"k1": ["a1", {"b": "c2"}], "plain": 3});
        rewrite_json_strings(&mut value, &hide_digits);
        assert_eq!(value, json!({"k#": ["a#", {"b": "c#"}], "plain": 3}));
    }

    /// Keys that get the same new name keep their values.
    #[test]
    fn colliding_keys_keep_both_values() {
        let mut value = json!({"k1": 1, "k2": 2, "k#": 0});
        rewrite_json_strings(&mut value, &hide_digits);
        assert_eq!(value, json!({"k#": 0, "k# (2)": 1, "k# (3)": 2}));
    }
}
