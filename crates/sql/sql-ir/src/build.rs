//! Constructors for inner nodes emitted by lowering and circuit rewrites.
//! A child may define a type at this use or refer to a type defined elsewhere.
//! The completed circuit resolves references in the inner arena.

use crate::{Error, TypeKind};
use serde_json::{json, Value};

/// A type definition or an inner-arena reference in a type position.
pub struct TypeUse(Value);

impl TryFrom<Value> for TypeUse {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        let valid = value.as_object().is_some_and(|object| {
            if object.len() == 1 {
                return value["node"].as_u64().is_some();
            }
            value["id"].as_u64().is_some()
                && value["class"]
                    .as_str()
                    .and_then(TypeKind::from_wire)
                    .is_some()
        });
        if valid {
            Ok(Self(value))
        } else {
            Err(Error(
                "expected a circuit type definition or reference".into(),
            ))
        }
    }
}

/// The key changes both the DBSP type and the collection implementation name.
pub enum CollectionType {
    ZSet { element: TypeUse },
    IndexedZSet { key: TypeUse, element: TypeUse },
}

impl CollectionType {
    pub fn into_wire(self, id: u64, position: Option<&Value>) -> Value {
        let (kind, name, code, key, element) = match self {
            Self::ZSet { element } => (TypeKind::ZSet, "WSet", "ZSET", None, element),
            Self::IndexedZSet { key, element } => (
                TypeKind::IndexedZSet,
                "IndexedWSet",
                "INDEXED_ZSET",
                Some(key),
                element,
            ),
        };
        let mut node = json!({"class":kind.wire_name(), "id":id, "sourceEmpty":position.is_none()});
        if let Some(position) = position {
            node["position"] = position.clone();
        }
        if let Some(key) = key {
            node["keyType"] = key.0;
        }
        node["elementType"] = element.0;
        node["name"] = name.into();
        node["code"] = code.into();
        node["mayBeNull"] = false.into();
        node
    }
}
