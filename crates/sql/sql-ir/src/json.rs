//! Decode a compiler artifact while enforcing unique keys at every nesting level.

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{map::Entry, Map, Value};
use std::{cell::Cell, fmt};

#[derive(Clone, Copy)]
pub(crate) struct CheckedValue<'a> {
    needs_codec: &'a Cell<bool>,
}

impl<'a> CheckedValue<'a> {
    pub(crate) fn new(needs_codec: &'a Cell<bool>) -> Self {
        Self { needs_codec }
    }
}

impl<'de> DeserializeSeed<'de> for CheckedValue<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, input: D) -> Result<Value, D::Error> {
        input.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for CheckedValue<'_> {
    type Value = Value;

    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("JSON with unique object keys")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Value, M::Error> {
        let mut fields = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            // serde_json passes arbitrary-precision numbers through Serde as
            // one-entry maps. Parse the original numeric spelling exactly.
            if fields.is_empty() && key == "$serde_json::private::Number" {
                let digits: String = map.next_value()?;
                let number = digits.parse().map_err(de::Error::custom)?;
                if let Some(extra) = map.next_key::<String>()? {
                    let message = if extra == key {
                        "duplicate JSON object key"
                    } else {
                        "extra JSON number field"
                    };
                    return Err(de::Error::custom(message));
                }
                return Ok(Value::Number(number));
            }
            // A literal key can also activate an optional serde_json feature
            // (RawValue). Let the configured codec interpret the original bytes
            // once all object keys in the document have been checked.
            if fields.is_empty() && key.starts_with("$serde_json::private::") {
                self.needs_codec.set(true);
            }
            match fields.entry(key) {
                Entry::Occupied(_) => return Err(de::Error::custom("duplicate JSON object key")),
                Entry::Vacant(slot) => {
                    slot.insert(map.next_value_seed(self)?);
                }
            }
        }
        Ok(Value::Object(fields))
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Value, S::Error> {
        let mut items = Vec::new();
        while let Some(value) = seq.next_element_seed(self)? {
            items.push(value);
        }
        Ok(Value::Array(items))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }
}

/// Decode a compiler artifact without accepting ambiguous object keys or
/// rounding high-precision decimals. Uses serde_json's default nesting limit.
pub fn parse_json(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let needs_codec = Cell::new(false);
    let mut input = serde_json::Deserializer::from_slice(bytes);
    let value = CheckedValue::new(&needs_codec).deserialize(&mut input)?;
    input.end()?;
    if needs_codec.get() {
        drop(value);
        serde_json::from_slice(bytes)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_json;

    #[test]
    fn duplicate_keys_cannot_hide_definitions_or_differences() {
        for input in [
            r#"{"type": {"id": 1}, "type": {"node": 1}}"#,
            r#"[{"x": 1, "\u0078": 2}]"#,
        ] {
            assert!(parse_json(input.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("duplicate"));
        }
    }

    #[test]
    fn codec_reserved_fields_follow_the_enabled_serde_json_features() {
        for input in [
            r#"{"$serde_json::private::Number":"1.00300e50"}"#,
            r#"{"$serde_json::private::Number":17}"#,
            r#"{"$serde_json::private::Number":"21","extra":true}"#,
            r#"{"$serde_json::private::RawValue":"42"}"#,
            r#"{"$serde_json::private::RawValue":{"$serde_json::private::RawValue":"42"}}"#,
            r#"{"x":1,"$serde_json::private::Number":false}"#,
        ] {
            assert_eq!(
                parse_json(input.as_bytes()).ok(),
                serde_json::from_str::<serde_json::Value>(input).ok(),
                "{input}"
            );
        }
        assert!(parse_json(br#"{"$serde_json::private::RawValue":{"x":0,"x":1}}"#).is_err());
    }

    #[test]
    fn exact_numbers_and_all_json_scalars_survive() {
        let input =
            r#"[1234567890123456789012345678.1234567890,1e400,-2,3,true,false,null,"x",{},[]]"#;
        let value = parse_json(input.as_bytes()).unwrap();
        assert_eq!(
            value[0].to_string(),
            "1234567890123456789012345678.1234567890"
        );
        assert_eq!(value[1].to_string(), "1e+400");
        assert!(parse_json(b"{} {}").is_err());
    }
}
