//! Decode the Java writer's header-first objects directly into the circuit arena.
//! Other JSON producers can order object keys differently. The general decoder
//! resolves their node headers and port namespaces after reading the whole object.

use super::{insert_node, validated, Arena};
use crate::{json::CheckedValue, Circuit, Error, Field, Node, NodeClass, NodeId};
use indexmap::IndexMap;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::{borrow::Cow, cell::Cell, fmt};

/// Headers can usually borrow their text directly from the input buffer.
#[derive(serde::Deserialize)]
struct Text<'a>(#[serde(borrow)] Cow<'a, str>);

impl Circuit {
    /// Decode a graph for a single in-process optimizer call. Relational origins
    /// may refer to snapshots retained by that call's Java writer.
    pub fn from_live_json_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut circuit = Self::from_json_bytes(bytes)?;
        circuit.live_relations = true;
        Ok(circuit)
    }

    /// Parse a UTF-8 circuit and check object keys, identities and edges.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if let Ok(circuit) = decode_ordered(bytes) {
            return Ok(circuit);
        }
        // The general path also reports the same diagnostics as artifact import.
        Self::from_json(crate::parse_json(bytes).map_err(|e| Error::new(e.to_string()))?)
    }
}

fn decode_ordered(bytes: &[u8]) -> Result<Circuit, Error> {
    let mut nodes = Arena::new();
    let mut input = serde_json::Deserializer::from_slice(bytes);
    let needs_codec = Cell::new(false);
    let root = ReadField {
        nodes: &mut nodes,
        outer: true,
        depth: 0,
        needs_codec: &needs_codec,
    }
    .deserialize(&mut input)
    .map_err(|e| Error::new(e.to_string()))?;
    input.end().map_err(|e| Error::new(e.to_string()))?;
    let Field::Definition(root) = root else {
        return Err(Error::new("expected a circuit root definition"));
    };
    if needs_codec.get() || nodes[&root].class() != "DBSPCircuit" {
        return Err(Error::new("expected a circuit in writer order"));
    }
    validated(root, nodes)
}

struct ReadField<'a> {
    nodes: &'a mut Arena,
    outer: bool,
    depth: usize,
    needs_codec: &'a Cell<bool>,
}

impl ReadField<'_> {
    fn child(&mut self, outer: bool) -> ReadField<'_> {
        ReadField {
            nodes: self.nodes,
            outer,
            depth: self.depth + 1,
            needs_codec: self.needs_codec,
        }
    }

    fn id(&self, n: u64) -> NodeId {
        if self.outer {
            NodeId::Outer(n)
        } else {
            NodeId::Inner(n)
        }
    }
}

impl<'de> DeserializeSeed<'de> for ReadField<'_> {
    type Value = Field;

    fn deserialize<D: Deserializer<'de>>(self, input: D) -> Result<Field, D::Error> {
        super::check_depth(self.depth).map_err(de::Error::custom)?;
        input.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ReadField<'_> {
    type Value = Field;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a circuit field in writer order")
    }

    fn visit_map<M: MapAccess<'de>>(mut self, mut map: M) -> Result<Field, M::Error> {
        let mut key = map.next_key::<Text<'de>>()?.map(|text| text.0);
        if key.as_deref() == Some("node") {
            let id = self.id(map.next_value()?);
            if map.next_key::<de::IgnoredAny>()?.is_some() {
                return Err(de::Error::custom("expected a single reference field"));
            }
            return Ok(Field::Reference(id));
        }
        let header = if key.as_deref() == Some("class") {
            let class = map.next_value::<Text<'de>>()?.0;
            if map
                .next_key::<Text<'de>>()?
                .as_ref()
                .map(|key| key.0.as_ref())
                != Some("id")
            {
                return Err(de::Error::custom("expected node id after class"));
            }
            let id = self.id(map.next_value()?);
            key = map.next_key::<Text<'de>>()?.map(|text| text.0);
            Some((id, NodeClass::decode(id, class).map_err(de::Error::custom)?))
        } else {
            None
        };
        // A typed node usually has three to six fields after its header.
        let mut fields = IndexMap::with_capacity(if header.is_some() { 6 } else { 0 });
        while let Some(name) = key {
            if matches!(name.as_ref(), "class" | "id") || name.starts_with("$serde_json::private::")
            {
                return Err(de::Error::custom("field requires general decoding"));
            }
            let port = name == "operator" && fields.contains_key("outputNumber");
            if name == "operator" && !port {
                return Err(de::Error::custom("expected output number before operator"));
            }
            let opaque = super::opaque_field(&name, self.depth, self.outer && header.is_some());
            let outer = super::outer_child(&name, port);
            let indexmap::map::Entry::Vacant(slot) = fields.entry(name.into_owned()) else {
                return Err(de::Error::custom("duplicate JSON object key"));
            };
            let field = if opaque {
                Field::Data(map.next_value_seed(CheckedValue::new(self.needs_codec))?)
            } else {
                map.next_value_seed(self.child(outer))?
            };
            slot.insert(field);
            key = map.next_key::<Text<'de>>()?.map(|text| text.0);
        }
        if let Some((id, class)) = header {
            insert_node(self.nodes, id, Node::new(class, fields)).map_err(de::Error::custom)?;
            Ok(Field::Definition(id))
        } else {
            Ok(Field::Object(fields))
        }
    }

    fn visit_seq<S: SeqAccess<'de>>(mut self, mut seq: S) -> Result<Field, S::Error> {
        let mut items = Vec::new();
        while let Some(value) = seq.next_element_seed(self.child(self.outer))? {
            items.push(value);
        }
        Ok(Field::Array(items))
    }

    fn visit_unit<E>(self) -> Result<Field, E> {
        Ok(Field::Data(Value::Null))
    }
    fn visit_bool<E>(self, v: bool) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
    fn visit_f64<E>(self, v: f64) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
    fn visit_str<E>(self, v: &str) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
    fn visit_string<E>(self, v: String) -> Result<Field, E> {
        Ok(Field::Data(v.into()))
    }
}
