//! Configuration errors without the rejected values, which can be secrets.

/// Describe a deserialization failure without the rejected input.
///
/// Serde's messages quote the rejected scalar, enum variant, or map key,
/// and a custom deserializer can copy arbitrary input into its message. The
/// parts that come from the schema stay: the list of expected values or
/// fields, the name of a missing or repeated field, and the position that
/// serde_json appends. Callers supply the endpoint or source position
/// separately.
///
/// This is an allow list over the messages of serde's `de::Error`
/// constructors: `invalid_type`, `invalid_value`, `invalid_length`,
/// `unknown_variant`, `unknown_field`, `missing_field` and
/// `duplicate_field`. Any other message, such as one from `custom`, becomes
/// a generic text: an unknown shape loses detail but never shows a value.
pub fn deserialization_error(error: &impl std::fmt::Display) -> String {
    let error = error.to_string();
    // The rejected token comes before the last ", expected ": the expected
    // part is schema-defined and never holds it.
    let expected = || {
        error
            .rsplit_once(", expected ")
            .map(|(_, expected)| expected)
    };
    if error.starts_with("invalid type:") {
        match expected() {
            Some(expected) => {
                format!("a configuration value has the wrong type; expected {expected}")
            }
            None => "a configuration value has the wrong type; check the documented field types"
                .to_string(),
        }
    } else if error.starts_with("invalid value:") || error.starts_with("unknown variant ") {
        match expected() {
            Some(expected) => {
                format!("a configuration value is not supported; expected {expected}")
            }
            None => "a configuration value is not supported; check the documented allowed values"
                .to_string(),
        }
    } else if let Some(rest) = error.strip_prefix("unknown field ") {
        // The field name is an option name the user typed, not a value:
        // keep it when it looks like one, so the user knows what to fix.
        let token = rest
            .rsplit_once(", expected ")
            .map_or(rest, |(token, _)| token);
        let token = token.split(" at line ").next().unwrap_or_default();
        let name = option_name(token).map_or(String::new(), |name| format!(" {name}"));
        match expected() {
            Some(expected) => {
                format!("the configuration contains an unknown field{name}; expected {expected}")
            }
            None => format!(
                "the configuration contains an unknown field{name}; check the documented option names"
            ),
        }
    } else if error.starts_with("invalid length ")
        || error.starts_with("missing field ")
        || error.starts_with("duplicate field ")
    {
        // The length is a number, and the field names are schema-defined.
        error
    } else {
        "the configuration could not be parsed; check its syntax and documented options".to_string()
    }
}

/// `token` when it is a backquoted option name: letters, digits, `_`, `.`
/// and `-`, at most 64 characters.
fn option_name(token: &str) -> Option<&str> {
    let name = token.strip_prefix('`')?.strip_suffix('`')?;
    let plain = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
    (!name.is_empty() && name.len() <= 64 && plain).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::deserialization_error;
    use crate::format::avro::{AvroEncoderConfig, AvroUpdateFormat};
    use serde_json::json;

    /// The rejected input is left out; the schema-defined parts stay.
    #[test]
    fn rejected_input_is_left_out_and_the_expected_part_stays() {
        #[derive(Debug, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Options {
            update_format: Option<AvroUpdateFormat>,
            threads: Option<usize>,
        }
        let cases = [
            (json!("hunter2"), "wrong type; expected struct Options"),
            (
                json!({"update_format": "hunter2"}),
                "not supported; expected one of `raw`, `debezium`",
            ),
            (json!({"threads": "hunter2"}), "wrong type; expected usize"),
        ];
        for (config, expected) in cases {
            let error = serde_json::from_value::<Options>(config).unwrap_err();
            let reason = deserialization_error(&error);
            assert!(!reason.contains("hunter2"), "{reason}");
            assert!(reason.contains(expected), "{reason}");
        }
        // An unknown field is an option name the user typed: it is named.
        let error = serde_json::from_value::<Options>(json!({"treads": true})).unwrap_err();
        assert_eq!(
            deserialization_error(&error),
            "the configuration contains an unknown field `treads`; expected `update_format` or `threads`"
        );
        let error = serde_json::from_value::<Options>(json!({"a b": true})).unwrap_err();
        assert!(!deserialization_error(&error).contains("a b"));
    }

    /// A struct with a flattened field gets no expected list from serde;
    /// the category and the advice remain.
    #[test]
    fn a_flattened_config_keeps_the_category() {
        let error =
            serde_json::from_value::<AvroEncoderConfig>(json!({"hunter2": true})).unwrap_err();
        assert_eq!(
            deserialization_error(&error),
            "the configuration contains an unknown field `hunter2`; check the documented option names"
        );
    }

    /// Serde's position suffix and the schema-defined field names stay.
    #[test]
    fn positions_and_schema_field_names_stay() {
        let error = serde_json::from_str::<AvroEncoderConfig>(
            r#"{"namespace": "hunter2", "namespace": "a"}"#,
        )
        .unwrap_err();
        assert_eq!(
            deserialization_error(&error),
            "duplicate field `namespace` at line 1 column 36"
        );
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Required {
            key: String,
        }
        let error = serde_json::from_str::<Required>(r#"{"hunter2": "x"}"#).unwrap_err();
        assert_eq!(
            deserialization_error(&error),
            "missing field `key` at line 1 column 16"
        );
    }

    /// A custom deserializer error can hold anything: none of it is copied,
    /// also when it contains ", expected ".
    #[test]
    fn custom_errors_are_not_copied() {
        for error in ["custom error: hunter2", "hunter2, expected hunter2"] {
            let reason = deserialization_error(&error);
            assert!(!reason.contains("hunter2"), "{reason}");
            assert!(reason.contains("check its syntax"), "{reason}");
        }
    }
}
