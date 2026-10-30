//! Configuration errors without the rejected values, which can be secrets.

/// Describe a deserialization failure using only reviewed, fixed text.
///
/// Serde's messages can quote a rejected scalar, enum variant, or map key.
/// Custom deserializers can also include arbitrary input in their messages.
/// Keep a useful error category, but never copy any part of that input into
/// a diagnostic. Callers supply the endpoint or source position separately.
pub fn deserialization_error(error: &impl std::fmt::Display) -> &'static str {
    let error = error.to_string();
    if error.starts_with("invalid type:") {
        "a configuration value has the wrong type; check the documented field types"
    } else if error.starts_with("invalid value:") || error.starts_with("unknown variant ") {
        "a configuration value is not supported; check the documented allowed values"
    } else if error.starts_with("unknown field ") {
        "the configuration contains an unknown field; check the documented option names"
    } else if error.starts_with("missing field ") {
        "a required configuration field is missing; check the documented required options"
    } else if error.starts_with("duplicate field ") {
        "a configuration field is repeated; specify each option once"
    } else if error.starts_with("invalid Avro schema:") {
        "the Avro schema is invalid; check the format's schema option against the Avro specification"
    } else {
        "the configuration could not be parsed; check its syntax and documented options"
    }
}

#[cfg(test)]
mod tests {
    use super::deserialization_error;
    use crate::format::avro::AvroEncoderConfig;
    use serde_json::json;

    #[test]
    fn rejected_scalars_fields_and_custom_errors_are_not_copied() {
        for config in [
            json!("hunter2"),
            json!({"registry_urls": "hunter2"}),
            json!({"update_format": "hunter2"}),
            json!({"hunter2": true}),
        ] {
            let error = serde_json::from_value::<AvroEncoderConfig>(config).unwrap_err();
            let reason = deserialization_error(&error);
            assert!(!reason.contains("hunter2"), "{reason}");
            assert!(reason.contains("check"), "{reason}");
        }
        assert!(!deserialization_error(&"custom error: hunter2").contains("hunter2"));
    }
}
