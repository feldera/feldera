//! Decode a compiler artifact while enforcing unique keys at every nesting level.

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value, map::Entry};
use std::{cell::Cell, fmt};

#[derive(Clone, Copy)]
pub(crate) struct CheckedValue<'a> {
    needs_codec: &'a Cell<bool>,
    depth: usize,
}

impl<'a> CheckedValue<'a> {
    pub(crate) fn new(needs_codec: &'a Cell<bool>) -> Self {
        Self::at_depth(needs_codec, 0)
    }

    pub(crate) fn at_depth(needs_codec: &'a Cell<bool>, depth: usize) -> Self {
        Self { needs_codec, depth }
    }

    fn child(self) -> Self {
        Self {
            depth: self.depth + 1,
            ..self
        }
    }
}

// A detached field starts a new serde_json deserializer. At the codec's
// recursion boundary, reparse the entire document with its original budget.
pub(crate) fn check_document_depth(depth: usize, needs_codec: &Cell<bool>) {
    if depth >= 127 {
        needs_codec.set(true);
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
        check_document_depth(self.depth, self.needs_codec);
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
                    slot.insert(map.next_value_seed(self.child())?);
                }
            }
        }
        Ok(Value::Object(fields))
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Value, S::Error> {
        check_document_depth(self.depth, self.needs_codec);
        let mut items = Vec::new();
        while let Some(value) = seq.next_element_seed(self.child())? {
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

// Jackson's default checkpoint reader accepts at most 1,000 nested containers.
pub(crate) const MAX_CIRCUIT_DEPTH: usize = 1000;

/// Decode a compiler graph with deep inline definitions. Ordinary artifacts
/// use the same checked decoder as before. The deep path keeps its traversal
/// on the heap, including for producers that reorder node headers.
pub fn parse_circuit_json(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    if let Ok(value) = parse_json(bytes) {
        return Ok(value);
    }
    use serde_json::value::RawValue;

    struct Members;
    impl<'de> Visitor<'de> for Members {
        type Value = indexmap::IndexMap<String, &'de RawValue>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON object with unique keys")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut fields = Self::Value::new();
            while let Some((name, raw)) = map.next_entry()? {
                if fields.insert(name, raw).is_some() {
                    return Err(de::Error::custom("duplicate JSON object key"));
                }
            }
            Ok(fields)
        }
    }

    enum Step<'a> {
        Read(&'a RawValue, usize),
        Array(usize),
        Object(Vec<String>),
    }
    let root: &RawValue = serde_json::from_slice(bytes)?;
    let mut steps = vec![Step::Read(root, 0)];
    let mut values = Vec::new();
    while let Some(step) = steps.pop() {
        let value = match step {
            Step::Read(raw, depth) => {
                let bytes = raw.get().as_bytes();
                let container = matches!(bytes.first(), Some(b'{' | b'['));
                if container && depth >= MAX_CIRCUIT_DEPTH {
                    return Err(de::Error::custom("circuit exceeds 1000 nested containers"));
                }
                // A fragment under 256 bytes contains at most 127 nested containers.
                // Near the overall limit, count even these small containers individually.
                if !container || (bytes.len() < 256 && depth <= MAX_CIRCUIT_DEPTH - 128) {
                    values.push(parse_json(bytes)?);
                    continue;
                }
                match bytes[0] {
                    b'{' => {
                        let fields =
                            serde_json::Deserializer::from_slice(bytes).deserialize_map(Members)?;
                        if fields
                            .first()
                            .is_some_and(|(k, _)| k.starts_with("$serde_json::private::"))
                        {
                            // Preserve the configured codec's interpretation of its reserved fields.
                            parse_json(bytes)?
                        } else {
                            let (keys, children): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
                            steps.push(Step::Object(keys));
                            steps.extend(
                                children
                                    .into_iter()
                                    .rev()
                                    .map(|raw| Step::Read(raw, depth + 1)),
                            );
                            continue;
                        }
                    }
                    b'[' => {
                        let children: Vec<&RawValue> = serde_json::from_slice(bytes)?;
                        steps.push(Step::Array(children.len()));
                        steps.extend(
                            children
                                .into_iter()
                                .rev()
                                .map(|raw| Step::Read(raw, depth + 1)),
                        );
                        continue;
                    }
                    _ => unreachable!("checked container"),
                }
            }
            Step::Array(len) => Value::Array(values.split_off(values.len() - len)),
            Step::Object(keys) => {
                let fields = values.drain(values.len() - keys.len()..);
                Value::Object(keys.into_iter().zip(fields).collect())
            }
        };
        values.push(value);
    }
    Ok(values.pop().expect("one JSON root"))
}

#[cfg(test)]
mod tests {
    use super::{MAX_CIRCUIT_DEPTH, parse_circuit_json, parse_json};

    #[test]
    fn circuit_reader_counts_containers_instead_of_brackets_in_string_values() {
        fn document(depth: usize) -> String {
            format!(r#"{}"text with [ and \"{{"{}"#, "[".repeat(depth), "]".repeat(depth))
        }
        let mut value = parse_circuit_json(document(MAX_CIRCUIT_DEPTH).as_bytes()).unwrap();
        for _ in 0..MAX_CIRCUIT_DEPTH {
            value = value.as_array_mut().unwrap().pop().unwrap();
        }
        assert_eq!(value, "text with [ and \"{");
        let err = parse_circuit_json(document(MAX_CIRCUIT_DEPTH + 1).as_bytes()).unwrap_err();
        assert!(err.to_string().contains("1000 nested containers"));
    }

    #[test]
    fn duplicate_keys_cannot_hide_definitions_or_differences() {
        for input in [
            r#"{"type": {"id": 1}, "type": {"node": 1}}"#,
            r#"[{"x": 1, "\u0078": 2}]"#,
        ] {
            assert!(
                parse_json(input.as_bytes())
                    .unwrap_err()
                    .to_string()
                    .contains("duplicate")
            );
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
