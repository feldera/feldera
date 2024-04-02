#![deny(missing_docs)]

//! Functions related to `JSON` support in DBSP

/// The actual Json type.
pub type Json = ijson::IValue;

/// Parses the given string to [`Json`].
///
/// # Panics
/// - Panics if it fails to deserialize to a valid Json.
pub fn parse_json(value: impl AsRef<str>) -> Json {
    serde_json::from_str(value.as_ref()).unwrap_or_else(|_| {
        panic!(
            "cannot deserialize given string to JSON: {}",
            value.as_ref()
        )
    })
}

/// Returns `true` if the given string is a valid [`Json`].
pub fn check_json(value: impl AsRef<str>) -> bool {
    serde_json::from_str::<Json>(value.as_ref()).is_ok()
}

/// Extracts a `field` from the [`Json`] value.
/// Use [`try_json_field`] for a non panicking implementation.
///
/// # Panics
/// - Panics if the JSON value isn't a JSON object.
/// - Panics if the `field` doesn't exist in this object.
pub fn json_field(value: Json, field: impl AsRef<str>) -> Json {
    value
        .get(field.as_ref())
        .expect("invalid: JSON_FIELD called with non existent field")
        .clone()
}

/// Tries to extract a `field` from the [`Json`] value.
pub fn try_json_field(value: Json, field: impl AsRef<str>) -> Option<Json> {
    value.as_object()?.get(field.as_ref()).cloned()
}

/// Extracts the value at the given JSON index from this JSON array literal.
/// Indexing starts from 1 to match other SQL functions.
/// Use [`try_json_index`] for a non panicking implementation.
///
/// # Panics
/// - Panics if the JSON value isn't a JSON array literal.
/// - Panics if called with the index `0`.
pub fn json_index(value: Json, mut idx: usize) -> Json {
    idx = idx
        .checked_sub(1)
        .expect("invalid: JSON_INDEX called with index 0");

    value
        .get(idx)
        .expect("invalid: no value in the given index")
        .clone()
}

/// Tries to extract the value at the given index from this JSON array literal.
pub fn try_json_index(value: Json, mut idx: usize) -> Option<Json> {
    idx = idx.checked_sub(1)?;

    value.as_array()?.get(idx).cloned()
}

/// Deserialize this JSON `value` as the given type: `T`.
/// `T` must have implement [`serde::de::DeserializeOwned`]
///
/// # Panics
/// - Panics if deserializing to the given type `T` fails.
pub fn json_as<T: serde::de::DeserializeOwned>(value: Json) -> T {
    ijson::from_value(&value).expect("failed to deserialize JSON to the given type")
}

/// Serialize this JSON `value` as a string.
///
/// # Panics
/// - Panics if serialization fails.
pub fn to_string(value: Json) -> String {
    serde_json::to_string(&value).expect("failed to cast JSON to string")
}

// TODO: check_schema()

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ijson::ijson;

    use crate::{check_json, json_as, json_index, parse_json, to_string};

    #[test]
    fn test_parse_json0() {
        let value = parse_json("[1, 2, 3]");

        let got: i32 = json_as(json_index(value, 3));

        assert_eq!(3, got);
    }

    #[test]
    #[should_panic(expected = "cannot deserialize given string to JSON")]
    fn test_parse_json_fail0() {
        parse_json("[1, 2, 3,]");
    }

    #[test]
    fn test_check_json0() {
        assert!(check_json("[1, 2, 3]"));
    }

    #[test]
    fn test_check_json1() {
        assert!(!check_json("[1, 2, 3,]"));
    }

    #[test]
    #[should_panic(expected = "index 0")]
    fn test_json_index_fail() {
        let json = ijson!([1, 2, 3]);
        json_index(json, 0);
    }

    #[test]
    fn test_json_as_array() {
        let expected = vec![1, 2, 3];
        let json = ijson!(expected);

        let got = json_as::<Vec<i32>>(json);

        assert_eq!(expected, got);
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct TestData {
        name: String,
        age: u32,
    }

    #[test]
    fn test_json_as_struct() {
        let json_value = ijson!({
            "name": "Alice",
            "age": 30
        });

        let data: TestData = json_as(json_value);

        assert_eq!(
            data,
            TestData {
                name: "Alice".to_string(),
                age: 30
            }
        );
    }

    #[test]
    #[should_panic(expected = "failed to deserialize JSON to the given type")]
    fn test_json_as_struct_fail() {
        let invalid_json_value = ijson!({
            "name": "Bob"
        });

        let _ = json_as::<TestData>(invalid_json_value);
    }

    #[test]
    fn test_json_as_hashmap() {
        let json = ijson!({
            "name": "Bob",
        });

        let _ = json_as::<HashMap<String, String>>(json);
    }

    #[test]
    fn test_to_string() {
        let expected = r#"{"name":"Bob","age":30}"#;

        let json = parse_json(expected);
        let got = to_string(json);

        assert_eq!(expected, got);
    }
}
