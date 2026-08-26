//! Tolerant accessors for raw API payloads.
//!
//! Every reader in this module returns a default instead of an error: the
//! console must keep rendering against servers that are older or newer than
//! this build, so a missing or re-typed field can never abort a refresh.

use serde_json::Value;

/// Read a string field, accepting numbers so that version skew in a field's
/// type does not blank the cell.
pub fn string(value: &Value, key: &str) -> Option<String> {
    match value.get(key)? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// Read an integer field, accepting floats and numeric strings.
pub fn integer(value: &Value, key: &str) -> Option<i64> {
    match value.get(key)? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|float| float as i64)),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

/// Read an integer field, defaulting to zero.
pub fn integer_or_zero(value: &Value, key: &str) -> i64 {
    integer(value, key).unwrap_or(0)
}

/// Read a float field, accepting integers and numeric strings.
pub fn float(value: &Value, key: &str) -> Option<f64> {
    match value.get(key)? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

/// Read a boolean field.
pub fn boolean(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

/// Read an array field, defaulting to empty.
pub fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// Parse a timestamp field into epoch milliseconds. Servers send either
/// RFC 3339 strings or raw epoch milliseconds, depending on the endpoint.
pub fn timestamp_millis(value: &Value, key: &str) -> Option<i64> {
    match value.get(key)? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|instant| instant.timestamp_millis()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{array, boolean, float, integer, integer_or_zero, string, timestamp_millis};
    use serde_json::json;

    #[test]
    fn readers_survive_missing_and_mistyped_fields() {
        let value = json!({"name": 7, "count": "42", "rate": 1, "flag": "yes", "items": 3});
        assert_eq!(string(&value, "name"), Some("7".to_string()));
        assert_eq!(string(&value, "absent"), None);
        assert_eq!(integer(&value, "count"), Some(42));
        assert_eq!(integer(&value, "name"), Some(7));
        assert_eq!(integer(&value, "items"), Some(3));
        assert_eq!(integer_or_zero(&value, "absent"), 0);
        assert_eq!(float(&value, "rate"), Some(1.0));
        assert_eq!(float(&value, "count"), Some(42.0));
        assert!(!boolean(&value, "flag"));
        assert!(array(&value, "items").is_empty());
    }

    #[test]
    fn integer_accepts_floats_and_rejects_garbage() {
        let value = json!({"float": 1.9, "text": "abc"});
        assert_eq!(integer(&value, "float"), Some(1));
        assert_eq!(integer(&value, "text"), None);
        assert_eq!(float(&value, "text"), None);
    }

    #[test]
    fn timestamps_parse_rfc3339_and_epoch_millis() {
        let value = json!({
            "good": "2026-01-02T03:04:05Z",
            "epoch": 1_787_564_660_666i64,
            "bad": "yesterday",
            "list": []
        });
        assert_eq!(timestamp_millis(&value, "good"), Some(1_767_323_045_000));
        assert_eq!(timestamp_millis(&value, "epoch"), Some(1_787_564_660_666));
        assert_eq!(timestamp_millis(&value, "bad"), None);
        assert_eq!(timestamp_millis(&value, "list"), None);
    }

    #[test]
    fn booleans_and_arrays_read_their_native_types() {
        let value = json!({"flag": true, "items": [1, 2]});
        assert!(boolean(&value, "flag"));
        assert_eq!(array(&value, "items").len(), 2);
    }
}
