//! Finds configuration settings that deserialization ignores.
//!
//! [`serde_ignored`](https://crates.io/crates/serde_ignored) cannot see keys
//! inside `#[serde(flatten)]` structs, and [`PipelineConfig`] flattens
//! [`RuntimeConfig`], so this module uses a round trip instead:
//!
//! 1. Deserialize the input, serialize it again, and list each key in the
//!    input that is not in the output.  A key can disappear because it is
//!    unknown, but also because it is an alias or a default value that
//!    serialization skips.
//!
//! 2. For each such key, replace its value with a string that no setting
//!    accepts as is.  If the result deserializes to the same configuration,
//!    the key is ignored.  If deserialization fails or the configuration
//!    changes, then something read the key.
//!
//! [`PipelineConfig`]: super::PipelineConfig
//! [`RuntimeConfig`]: super::RuntimeConfig

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// Value put in place of a key to find out whether anything reads it.
///
/// The probe does not need to be rejected.  A string or `Value` setting accepts
/// it, but then the probe appears in the serialized output, which then differs
/// from the baseline.  Other settings fail to deserialize it.  Only a key that
/// nothing reads gives the baseline again.  This is wrong only for a setting
/// that accepts the probe and then discards it, such as one that turns bad
/// input into its default.
///
/// The `\0` only makes a match with a real value or enum variant unlikely.
const PROBE: &str = "\u{0}feldera-ignored-setting-probe";

/// Returns the paths of the keys in `input` that have no effect when it is
/// deserialized as a `T`, such as `inputs.t.transport.config.foo`.
///
/// Returns nothing if `input` does not deserialize as a `T`.  Does not look
/// inside values that `T` keeps as opaque JSON, such as format configuration.
pub fn ignored_settings<T>(input: &Value) -> Vec<String>
where
    T: DeserializeOwned + Serialize,
{
    let Some(baseline) = round_trip::<T>(input) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    find_dropped_keys(input, &baseline, &mut Vec::new(), &mut candidates);
    candidates
        .into_iter()
        .filter(|path| {
            let mut probe = input.clone();
            *lookup_mut(&mut probe, path) = Value::String(PROBE.into());
            round_trip::<T>(&probe).as_ref() == Some(&baseline)
        })
        .map(|path| format_path(&path))
        .collect()
}

#[derive(Clone)]
enum Segment {
    Key(String),
    Index(usize),
}

fn round_trip<T>(value: &Value) -> Option<Value>
where
    T: DeserializeOwned + Serialize,
{
    let t = serde_json::from_value::<T>(value.clone()).ok()?;
    serde_json::to_value(t).ok()
}

/// Appends to `dropped` the path of each key in `input` that `output` lacks.
fn find_dropped_keys(
    input: &Value,
    output: &Value,
    path: &mut Vec<Segment>,
    dropped: &mut Vec<Vec<Segment>>,
) {
    match (input, output) {
        (Value::Object(input), Value::Object(output)) => {
            for (key, value) in input {
                path.push(Segment::Key(key.clone()));
                match output.get(key) {
                    Some(output) => find_dropped_keys(value, output, path, dropped),
                    None => dropped.push(path.clone()),
                }
                path.pop();
            }
        }
        (Value::Array(input), Value::Array(output)) if input.len() == output.len() => {
            for (index, (input, output)) in input.iter().zip(output).enumerate() {
                path.push(Segment::Index(index));
                find_dropped_keys(input, output, path, dropped);
                path.pop();
            }
        }
        _ => (),
    }
}

fn lookup_mut<'a>(mut value: &'a mut Value, path: &[Segment]) -> &'a mut Value {
    for segment in path {
        value = match segment {
            Segment::Key(key) => &mut value[key.as_str()],
            Segment::Index(index) => &mut value[*index],
        };
    }
    value
}

fn format_path(path: &[Segment]) -> String {
    let mut s = String::new();
    for segment in path {
        match segment {
            Segment::Key(key) => {
                if !s.is_empty() {
                    s.push('.');
                }
                s.push_str(key);
            }
            Segment::Index(index) => s.push_str(&format!("[{index}]")),
        }
    }
    s
}

#[cfg(test)]
mod test {
    use super::ignored_settings;
    use crate::config::PipelineConfig;
    use serde_json::json;

    /// Sorted, because the order follows `serde_json::Map`, which keeps
    /// input order only with the `preserve_order` feature.
    fn ignored(input: serde_json::Value) -> Vec<String> {
        let mut settings = ignored_settings::<PipelineConfig>(&input);
        settings.sort();
        settings
    }

    #[test]
    fn known_settings_are_not_ignored() {
        assert_eq!(ignored(json!({})), Vec::<String>::new());
        assert_eq!(
            ignored(json!({
                "workers": 4,
                "storage": null,
                "max_rss_mb": 1024,
                "inputs": {
                    "t.unnamed-0": {
                        "stream": "t",
                        "transport": {"name": "datagen", "config": {"plan": [{}]}},
                    }
                },
            })),
            Vec::<String>::new()
        );
    }

    /// A default value that serialization skips is still a used setting.
    #[test]
    fn explicit_defaults_are_not_ignored() {
        assert_eq!(
            ignored(json!({
                "inputs": {
                    "c": {
                        "stream": "t",
                        "transport": {"name": "datagen", "config": {}},
                        "distributed": false,
                    }
                },
            })),
            Vec::<String>::new()
        );
    }

    #[test]
    fn unknown_settings_are_ignored() {
        assert_eq!(
            ignored(json!({
                "wokers": 4,
                "dev_tweaks": {"buffer_size": 1},
                "storage": {"bogus": null},
                "inputs": {
                    "t.unnamed-0": {
                        "stream": "t",
                        "transport": {
                            "name": "file_input",
                            "config": {"path": "x", "bogus": 2},
                        },
                        "bogus": 4,
                    }
                },
            })),
            vec![
                "inputs.t.unnamed-0.bogus",
                "inputs.t.unnamed-0.transport.config.bogus",
                "storage.bogus",
                "wokers",
            ]
        );
    }
}
