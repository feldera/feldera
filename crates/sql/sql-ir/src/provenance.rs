//! Relational origins of circuit operators. The optimizer can share both an
//! origin and the relation it describes. These identities occupy separate arenas.

mod sequence;

use crate::{Circuit, Error, Field, NodeId};
pub use feldera_sql_source::SourceRange;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

const MAX_ORIGIN_DEPTH: usize = 128;
const MAX_INTERMEDIATE_NODES: usize = 65_536;

impl Field {
    pub fn source_range(&self) -> Result<SourceRange, Error> {
        let Field::Object(fields) = self else {
            return Err(Error("invalid source position".into()));
        };
        let coordinate = |name: &str| {
            match fields.get(name) {
                Some(Field::Data(value)) => value.as_i64().and_then(|n| i32::try_from(n).ok()),
                _ => None,
            }
            .ok_or_else(|| Error(format!("invalid source position {name}")))
        };
        Ok(SourceRange {
            start_line_number: coordinate("start_line_number")?,
            start_column: coordinate("start_column")?,
            end_line_number: coordinate("end_line_number")?,
            end_column: coordinate("end_column")?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OriginId(u32);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RelationId(u32);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewOrigin<'a> {
    pub name: &'a str,
    pub case_sensitive: bool,
    pub position: SourceRange,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Relation<'a> {
    pub rel_id: i32,
    pub plan_id: i32,
    pub table_scan: bool,
    pub digest: &'a str,
    pub text: &'a str,
    pub sql: &'a str,
    pub stateful_sql_name: Option<&'a str>,
    #[serde(borrow)]
    pub origin: Option<ViewOrigin<'a>>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum OriginKind {
    Empty,
    PositionOnly { empty: bool },
    Final(RelationId),
    Intermediate(RelationId),
    Sequence(Vec<OriginId>),
    Intersection(Vec<OriginId>),
}

#[derive(Debug)]
pub struct Origin {
    pub kind: OriginKind,
    pub position: SourceRange,
    pub positions: Vec<SourceRange>,
}

#[derive(Debug)]
enum RelationRecord<'a> {
    Snapshot(Relation<'a>),
    Retained { plan_id: i32 },
}

/// Text is borrowed directly from the circuit. Only the indexes, source ranges,
/// and child lists are allocated when an optimizer asks for provenance.
#[derive(Debug, Default)]
pub struct Provenance<'a> {
    operators: BTreeMap<NodeId, OriginId>,
    origins: BTreeMap<OriginId, Origin>,
    relations: BTreeMap<RelationId, RelationRecord<'a>>,
    live_relations: bool,
}

impl Circuit {
    pub fn provenance(&self) -> Result<Provenance<'_>, Error> {
        let mut index = Provenance {
            live_relations: self.live_relations,
            ..Provenance::default()
        };
        index.visit_node(self, self.root)?;
        Ok(index)
    }

    /// Snapshot the provenance index before graph edits move its definitions.
    pub fn intermediate_origins(&self) -> Result<IntermediateOrigins<'_>, Error> {
        let table = self.provenance()?;
        let last = table.last_origin_number();
        Ok(IntermediateOrigins {
            circuit: self,
            table,
            last,
        })
    }
}

/// Allocate fresh origins in the identity space of a single transformation.
pub struct IntermediateOrigins<'a> {
    circuit: &'a Circuit,
    table: Provenance<'a>,
    last: u32,
}

impl IntermediateOrigins<'_> {
    pub fn derive<const N: usize>(&mut self, site: NodeId) -> Result<Option<[Value; N]>, Error> {
        let node = self
            .circuit
            .node(site)
            .ok_or_else(|| Error::new("unknown origin site"))?;
        if let Some(origin) = self.table.operator_origin(site) {
            let mut copies = [(); N].map(|_| Value::Null);
            for copy in &mut copies {
                *copy = self.table.intermediate(origin, &mut self.last)?;
            }
            return Ok(Some(copies));
        }
        if [
            "position",
            "sourcePosition",
            "sourcePositions",
            "sourceEmpty",
        ]
        .iter()
        .any(|key| node.field(key).is_some())
        {
            return Err(Error::new(
                "deriving an intermediate source position requires relational provenance",
            ));
        }
        Ok(None)
    }
}

impl<'a> Provenance<'a> {
    /// Allocate origins beyond the original checkpoint's identity range.
    pub(crate) fn last_origin_number(&self) -> u32 {
        self.origins.last_key_value().map_or(0, |(id, _)| id.0)
    }

    /// Derive the origin of an intermediate operator. Each call to Calcite's
    /// intermediate() constructs a new LastRel conversion or sequence. Relation
    /// identities and already-intermediate leaves stay shared.
    pub(crate) fn intermediate(&self, origin: OriginId, last: &mut u32) -> Result<Value, Error> {
        let mut remaining = MAX_INTERMEDIATE_NODES;
        self.intermediate_at(origin, last, 0, &mut remaining)
    }

    // A short chain of shared sequences can describe an exponentially larger tree.
    fn intermediate_at(
        &self,
        origin: OriginId,
        last: &mut u32,
        depth: usize,
        remaining: &mut usize,
    ) -> Result<Value, Error> {
        use serde_json::json;
        if depth > MAX_ORIGIN_DEPTH {
            return Err(Error::new(
                "intermediate provenance exceeds 128 nested origins",
            ));
        }
        *remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| Error::new("intermediate provenance exceeds the expansion budget"))?;
        let kind = &self.origins[&origin].kind;
        let mut node = match kind {
            OriginKind::Final(relation) => {
                json!({"kind":"IntermediateRel", "relation":{"ref":relation.0}})
            }
            OriginKind::Sequence(children) => {
                let children = children
                    .iter()
                    .map(|&child| self.intermediate_at(child, last, depth + 1, remaining))
                    .collect::<Result<Vec<_>, _>>()?;
                json!({"kind":"RelSequence", "children":children})
            }
            OriginKind::Empty | OriginKind::PositionOnly { .. } | OriginKind::Intermediate(_) => {
                return Ok(json!({"ref":origin.0}));
            }
            OriginKind::Intersection(_) => {
                return Err(Error::new("intermediate origin is undefined for RelAnd"))
            }
        };
        *last = last
            .checked_add(1)
            .filter(|&n| n <= i32::MAX as u32)
            .ok_or_else(|| Error::new("relational origin identity exhausted"))?;
        node["id"] = (*last).into();
        node["position"] = json!(SourceRange::default());
        node["positions"] = json!([]);
        Ok(node)
    }

    pub fn operator_origin(&self, operator: NodeId) -> Option<OriginId> {
        self.operators.get(&operator).copied()
    }

    /// Register a rebuilt operator whose relational origin is identical to its source.
    pub fn inherit_origin(&mut self, new: NodeId, source: NodeId) {
        if let Some(origin) = self.operator_origin(source) {
            self.operators.insert(new, origin);
        }
    }

    pub fn origin(&self, id: OriginId) -> Option<&Origin> {
        self.origins.get(&id)
    }

    pub fn relation(&self, id: RelationId) -> Option<&Relation<'a>> {
        match self.relations.get(&id)? {
            RelationRecord::Snapshot(relation) => Some(relation),
            RelationRecord::Retained { .. } => None,
        }
    }

    pub(super) fn plan_id(&self, id: RelationId) -> i32 {
        match &self.relations[&id] {
            RelationRecord::Snapshot(relation) => relation.plan_id,
            RelationRecord::Retained { plan_id } => *plan_id,
        }
    }

    fn visit_node(&mut self, circuit: &'a Circuit, id: NodeId) -> Result<(), Error> {
        for (key, field) in circuit.nodes[&id].fields.iter() {
            if key == "provenance" {
                if let Field::Data(value) = field {
                    let origin = self.read_origin(value, 0)?;
                    self.operators.insert(id, origin);
                }
            } else {
                self.visit_field(circuit, field)?;
            }
        }
        Ok(())
    }

    fn visit_field(&mut self, circuit: &'a Circuit, field: &'a Field) -> Result<(), Error> {
        match field {
            Field::Definition(id @ NodeId::Outer(_)) => self.visit_node(circuit, *id),
            Field::Array(items) => items
                .iter()
                .try_for_each(|item| self.visit_field(circuit, item)),
            Field::Object(fields) => fields
                .values()
                .try_for_each(|field| self.visit_field(circuit, field)),
            _ => Ok(()),
        }
    }

    fn read_relation(&mut self, value: &'a Value) -> Result<RelationId, Error> {
        if let Some(reference) = value.get("ref") {
            let id = RelationId(number(reference)?);
            return self
                .relations
                .contains_key(&id)
                .then_some(id)
                .ok_or_else(|| Error(format!("unknown provenance relation {id:?}")));
        }
        let id = RelationId(number(&value["id"])?);
        let relation = if let Some(slot) = value.get("retained") {
            if !self.live_relations {
                return Err(Error::new("a detached circuit cannot use a retained relation"));
            }
            number(slot)?;
            let plan_id = i32::deserialize(&value["planId"]).map_err(invalid)?;
            RelationRecord::Retained { plan_id }
        } else {
            RelationRecord::Snapshot(Relation::deserialize(value).map_err(invalid)?)
        };
        if self.relations.insert(id, relation).is_some() {
            return Err(Error(format!("duplicate provenance relation {id:?}")));
        }
        Ok(id)
    }

    fn read_origin(&mut self, value: &'a Value, depth: usize) -> Result<OriginId, Error> {
        if depth > MAX_ORIGIN_DEPTH {
            return Err(Error(
                "operator provenance exceeds 128 nested origins".into(),
            ));
        }
        if let Some(reference) = value.get("ref") {
            let id = OriginId(number(reference)?);
            return self
                .origins
                .contains_key(&id)
                .then_some(id)
                .ok_or_else(|| Error(format!("unknown provenance origin {id:?}")));
        }
        let id = OriginId(number(&value["id"])?);
        let position = SourceRange::deserialize(&value["position"]).map_err(invalid)?;
        let positions = Vec::<SourceRange>::deserialize(&value["positions"]).map_err(invalid)?;
        let kind = match value["kind"].as_str() {
            Some("CalciteEmptyRel") if !position.is_valid() && positions.is_empty() => {
                OriginKind::Empty
            }
            Some("PositionOnlyRel") => OriginKind::PositionOnly {
                empty: bool::deserialize(&value["empty"]).map_err(invalid)?,
            },
            Some("LastRel") => OriginKind::Final(self.read_relation(&value["relation"])?),
            Some("IntermediateRel") => {
                OriginKind::Intermediate(self.read_relation(&value["relation"])?)
            }
            Some(name @ ("RelSequence" | "RelAnd")) => {
                let children = value["children"]
                    .as_array()
                    .ok_or_else(|| Error(format!("{name} requires a children array")))?
                    .iter()
                    .map(|child| self.read_origin(child, depth + 1))
                    .collect::<Result<Vec<_>, _>>()?;
                if name == "RelSequence" {
                    OriginKind::Sequence(children)
                } else {
                    if children
                        .iter()
                        .any(|id| !matches!(self.origins[id].kind, OriginKind::Final(_)))
                    {
                        return Err(Error("RelAnd requires final relation origins".into()));
                    }
                    OriginKind::Intersection(children)
                }
            }
            other => {
                return Err(Error(format!(
                    "invalid operator provenance {other:?} at {id:?}"
                )))
            }
        };
        if self
            .origins
            .insert(
                id,
                Origin {
                    kind,
                    position,
                    positions,
                },
            )
            .is_some()
        {
            return Err(Error(format!("duplicate provenance origin {id:?}")));
        }
        Ok(id)
    }
}

fn number(value: &Value) -> Result<u32, Error> {
    value
        .as_u64()
        .filter(|id| *id <= i32::MAX as u64)
        .map(|id| id as u32)
        .ok_or_else(|| Error("expected a nonnegative 32-bit provenance identity".into()))
}

fn invalid(error: serde_json::Error) -> Error {
    Error(format!("invalid operator provenance: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_sequence_children_each_consume_the_expansion_budget() {
        let mut index = Provenance::default();
        for (id, kind) in [
            (0, OriginKind::Empty),
            (1, OriginKind::Sequence(vec![OriginId(0)])),
            (2, OriginKind::Sequence(vec![OriginId(1), OriginId(1)])),
        ] {
            index.origins.insert(
                OriginId(id),
                Origin {
                    kind,
                    position: SourceRange::default(),
                    positions: Vec::new(),
                },
            );
        }
        let root = OriginId(2);
        let error = index.intermediate_at(root, &mut 2, 0, &mut 4).unwrap_err();
        assert!(error.to_string().contains("expansion budget"));
        assert!(index.intermediate_at(root, &mut 2, 0, &mut 5).is_ok());
    }
}
