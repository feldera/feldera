//! Compiler-side decoding. Runtime serde cannot reproduce Jackson's scalar
//! coercions, first-error order, or flattened arbitrary connector options.

use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Deserialize)]
pub(crate) struct Inventory {
    pub classes: Vec<Schema>,
}

#[derive(Deserialize)]
pub(crate) struct Schema {
    pub class: String,
    pub prototype: bool,
    pub accepts_unknown: bool,
    pub fields: Vec<Field>,
}

#[derive(Deserialize)]
pub(crate) struct Field {
    pub name: String,
    pub java_type: String,
    pub default: Value,
    #[serde(default)]
    pub enum_values: Vec<String>,
}

pub(crate) struct DecodeError {
    /// Jackson currently joins unescaped map keys here. Keep that quirk visible
    /// to the location layer, including its fallback for '/' and '~' keys.
    pub path: String,
    pub message: String,
}

impl Schema {
    pub fn decode(&self, object: &Map<String, Value>) -> Result<Map<String, Value>, DecodeError> {
        let mut decoded: Map<_, _> = self
            .fields
            .iter()
            .map(|field| (field.name.clone(), field.default.clone()))
            .collect();
        for (name, value) in object {
            let Some(field) = self.fields.iter().find(|field| field.name == *name) else {
                if self.accepts_unknown {
                    continue;
                }
                return Err(DecodeError {
                    path: name.clone(),
                    message: format!("unknown field {name:?}"),
                });
            };
            decoded.insert(
                name.clone(),
                coerce(value, &field.java_type, &field.enum_values, name)?,
            );
        }
        Ok(decoded)
    }
}

fn coerce(
    value: &Value,
    kind: &str,
    variants: &[String],
    path: &str,
) -> Result<Value, DecodeError> {
    let invalid = || DecodeError {
        path: path.to_owned(),
        message: format!("field {path:?}: expected {kind}"),
    };
    if value.is_null() {
        return Ok(match kind {
            "boolean" => Value::Bool(false),
            "int" | "long" => Value::from(0),
            _ => Value::Null,
        });
    }
    if !variants.is_empty() {
        let selected = match value {
            Value::String(text) => {
                let text = text.trim_matches(|c: char| c <= ' ');
                if text.is_empty() {
                    return Err(invalid());
                }
                variants
                    .iter()
                    .find(|variant| variant.as_str() == text)
                    .or_else(|| {
                        text.parse::<usize>()
                            .ok()
                            .filter(|i| i.to_string() == text)
                            .and_then(|i| variants.get(i))
                    })
            }
            Value::Number(number) => number
                .as_u64()
                .and_then(|i| usize::try_from(i).ok())
                .and_then(|i| variants.get(i)),
            _ => None,
        };
        return selected
            .map(|text| Value::String(text.clone()))
            .ok_or_else(invalid);
    }
    match kind {
        "java.lang.Object" | "com.fasterxml.jackson.databind.JsonNode" => Ok(value.clone()),
        "java.lang.String" => match value {
            Value::String(_) => Ok(value.clone()),
            Value::Bool(_) | Value::Number(_) => Ok(Value::String(value.to_string())),
            _ => Err(invalid()),
        },
        "boolean" | "java.lang.Boolean" => {
            let boolean = match value {
                Value::Bool(b) => Some(*b),
                Value::Number(n) if !n.to_string().contains(['.', 'e', 'E']) => {
                    Some(n.to_string() != "0")
                }
                Value::String(s) => match s.trim_matches(|c: char| c <= ' ') {
                    "true" | "True" | "TRUE" => Some(true),
                    "false" | "False" | "FALSE" => Some(false),
                    "" | "null" => {
                        return Ok(if kind == "boolean" {
                            Value::Bool(false)
                        } else {
                            Value::Null
                        })
                    }
                    _ => None,
                },
                _ => None,
            };
            boolean.map(Value::Bool).ok_or_else(invalid)
        }
        "int" | "long" | "java.lang.Integer" | "java.lang.Long" => {
            let small = kind == "int" || kind == "java.lang.Integer";
            let integer = match value {
                Value::Number(n) => n.as_i64().or_else(|| {
                    // Jackson truncates floating-point numbers, but rejects a
                    // floating value outside the target type's range first.
                    if !n.to_string().contains(['.', 'e', 'E']) {
                        return None;
                    }
                    let n = n.as_f64()?;
                    let (min, max) = if small {
                        (i32::MIN as f64, i32::MAX as f64)
                    } else {
                        (i64::MIN as f64, i64::MAX as f64)
                    };
                    (n.is_finite() && n >= min && n <= max).then_some(n as i64)
                }),
                Value::String(s) => {
                    if matches!(s.trim_matches(|c: char| c <= ' '), "" | "null") {
                        return Ok(if kind == "int" || kind == "long" {
                            Value::from(0)
                        } else {
                            Value::Null
                        });
                    }
                    s.trim_matches(|c: char| c <= ' ').parse::<i64>().ok()
                }
                _ => None,
            };
            integer
                .filter(|i| !small || i32::try_from(*i).is_ok())
                .map(Value::from)
                .ok_or_else(invalid)
        }
        _ if kind.starts_with("java.util.List<") => {
            let element = &kind[15..kind.len() - 1];
            value
                .as_array()
                .ok_or_else(invalid)?
                .iter()
                .enumerate()
                .map(|(i, v)| coerce(v, element, &[], &format!("{path}/{i}")))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }
        "java.util.Map<java.lang.String, java.lang.String>" => value
            .as_object()
            .ok_or_else(invalid)?
            .iter()
            .map(|(k, v)| {
                coerce(v, "java.lang.String", &[], &format!("{path}/{k}")).map(|v| (k.clone(), v))
            })
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        _ => Err(invalid()),
    }
}

/// Java String.isBlank uses Character.isWhitespace, which excludes nonbreaking
/// spaces that Rust's Unicode whitespace predicate includes.
pub(crate) fn blank(value: &Value) -> bool {
    value.as_str().is_none_or(|s| s.chars().all(|c| {
        matches!(c, '\t'..='\r' | '\u{1c}'..='\u{20}' | '\u{1680}' |
            '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
    }))
}
