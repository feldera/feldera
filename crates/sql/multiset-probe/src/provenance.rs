//! Replay provenance has separate identities for origins and their relations.

use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

const MAX_NESTING: usize = 128;
const MAX_ID: u32 = i32::MAX as u32;
const MAX_INTERMEDIATE_NODES: usize = 65_536;

#[derive(Default)]
pub(crate) struct Provenance<'a> {
    origins: HashMap<u32, &'a Value>,
    relations: HashMap<u32, &'a Value>,
    next_origin: u32,
}

impl<'a> Provenance<'a> {
    pub(crate) fn read(operators: &'a [Value]) -> Result<Self> {
        let mut result = Self::default();
        result.read_operators(operators, 0)?;
        result.next_origin = result.origins.keys().copied().max().unwrap_or_default();
        Ok(result)
    }

    fn read_operators(&mut self, operators: &'a [Value], depth: usize) -> Result<()> {
        check_nesting(depth)?;
        for operator in operators {
            if let Some(origin) = operator.get("provenance") {
                self.read_origin(origin, 0)?;
            }
            if let Some(children) = operator.get("allOperators") {
                self.read_operators(
                    children
                        .as_array()
                        .ok_or_else(|| Error("invalid nested provenance scope".into()))?,
                    depth + 1,
                )?;
            }
        }
        Ok(())
    }

    fn read_origin(&mut self, wire: &'a Value, depth: usize) -> Result<()> {
        check_nesting(depth)?;
        if let Some(reference) = wire.get("ref") {
            lookup(&self.origins, id(reference)?)?;
            return Ok(());
        }

        match wire["kind"].as_str() {
            Some("LastRel" | "IntermediateRel") => {
                let relation = &wire["relation"];
                if let Some(reference) = relation.get("ref") {
                    lookup(&self.relations, id(reference)?)?;
                } else {
                    insert(&mut self.relations, relation)?;
                }
            }
            Some("RelSequence" | "RelAnd") => {
                for child in children(wire)? {
                    self.read_origin(child, depth + 1)?;
                }
            }
            Some("PositionOnlyRel" | "CalciteEmptyRel") => {}
            _ => return Err(Error("unknown replay provenance kind".into())),
        }

        // Java restores children before making the containing origin visible.
        insert(&mut self.origins, wire)
    }

    /// A synthetic operator with no SQL relation.
    pub(crate) fn empty_origin(&mut self) -> Result<Value> {
        Ok(json!({
            "id": self.allocate_origin()?,
            "kind": "CalciteEmptyRel",
            "position": {
                "start_line_number": 0, "start_column": 0,
                "end_line_number": 0, "end_column": 0,
            },
            "positions": [],
        }))
    }

    /// Add a scalar expression position to a copy of an existing origin.
    pub(crate) fn with_source_position(&mut self, wire: &Value, position: &Value) -> Result<Value> {
        let mut origin = resolve(&self.origins, wire)?.clone();
        let positions = origin["positions"]
            .as_array_mut()
            .ok_or_else(|| Error("missing replay provenance positions".into()))?;
        positions.push(position.clone());
        origin["id"] = self.allocate_origin()?.into();
        Ok(origin)
    }

    fn allocate_origin(&mut self) -> Result<u32> {
        if self.next_origin == MAX_ID {
            return Err(Error("no fresh identity for replay provenance".into()));
        }
        self.next_origin += 1;
        Ok(self.next_origin)
    }

    pub(crate) fn intermediate(&mut self, wire: &Value) -> Result<Value> {
        let mut budget = MAX_INTERMEDIATE_NODES;
        self.intermediate_at(wire, 0, &mut budget)
    }

    /// Compose the relational operations in execution order when fusing a chain.
    pub(crate) fn after(&mut self, later: &Value, earlier: &Value) -> Result<Value> {
        let later_origin = resolve(&self.origins, later)?;
        let earlier_origin = resolve(&self.origins, earlier)?;
        let later_kind = later_origin["kind"].as_str();
        let earlier_kind = earlier_origin["kind"].as_str();
        if later_kind == Some("RelAnd") {
            return Err(Error("cannot append a join origin to a chain".into()));
        }
        if empty(earlier_origin) {
            return Ok(later.clone());
        }
        if matches!(later_kind, Some("CalciteEmptyRel" | "PositionOnlyRel")) {
            return Ok(earlier.clone());
        }
        if earlier_kind == Some("RelAnd") {
            // The existing join origin already accounts for scan ancestry.
            return Ok(later.clone());
        }
        let previous = sequence_parts(earlier_origin)?;
        let following = sequence_parts(later_origin)?;
        let mut joined = Vec::with_capacity(previous.len() + following.len());
        if later_kind == Some("RelSequence") && earlier_kind == Some("RelSequence") {
            let mut present = std::collections::HashSet::new();
            for origin in previous {
                present.insert(self.relation_kind(origin)?);
                joined.push(as_reference(origin)?);
            }
            for origin in following {
                if present.insert(self.relation_kind(origin)?) {
                    joined.push(as_reference(origin)?);
                }
            }
        } else {
            let relations = following
                .iter()
                .map(|origin| self.relation_kind(origin).map(|key| key.1))
                .collect::<Result<Vec<_>>>()?;
            for origin in previous {
                if !relations.contains(&self.relation_kind(origin)?.1) {
                    joined.push(as_reference(origin)?);
                }
            }
            if joined.is_empty() && earlier_kind == Some("IntermediateRel") {
                return Ok(later.clone());
            }
            if joined.is_empty() && later_kind != Some("RelSequence") {
                return Ok(later.clone());
            }
            for origin in following {
                joined.push(as_reference(origin)?);
            }
        }
        joined.dedup();
        if self.next_origin == MAX_ID {
            return Err(Error("no fresh identity for sequence provenance".into()));
        }
        self.next_origin += 1;
        Ok(json!({
            "id": self.next_origin,
            "kind": "RelSequence",
            "position": {
                "start_line_number": 0, "start_column": 0,
                "end_line_number": 0, "end_column": 0,
            },
            "positions": [],
            "children": joined,
        }))
    }

    fn relation_kind(&self, wire: &Value) -> Result<(bool, u32)> {
        let node = resolve(&self.origins, wire)?;
        let is_last = match node["kind"].as_str() {
            Some("LastRel") => true,
            Some("IntermediateRel") => false,
            _ => return Err(Error("chain provenance requires relational leaves".into())),
        };
        Ok((is_last, identity(&node["relation"])?))
    }

    fn intermediate_at(&mut self, wire: &Value, depth: usize, budget: &mut usize) -> Result<Value> {
        check_nesting(depth)?;
        *budget = budget
            .checked_sub(1)
            .ok_or_else(|| Error("intermediate provenance exceeds the expansion budget".into()))?;
        let origin = resolve(&self.origins, wire)?;
        let kind = match origin["kind"].as_str() {
            Some("LastRel") => "IntermediateRel",
            Some("RelSequence") => "RelSequence",
            Some("IntermediateRel" | "PositionOnlyRel" | "CalciteEmptyRel") => {
                return as_reference(wire);
            }
            _ => return Err(Error("unsupported intermediate provenance".into())),
        };
        if self.next_origin == MAX_ID {
            return Err(Error(
                "no fresh identity for intermediate provenance".into(),
            ));
        }
        self.next_origin += 1;
        let mut intermediate = json!({
            "id": self.next_origin,
            "kind": kind,
            "position": {
                "start_line_number": 0, "start_column": 0,
                "end_line_number": 0, "end_column": 0,
            },
            "positions": [],
        });
        if kind == "RelSequence" {
            let children = children(origin)?
                .iter()
                .map(|child| self.intermediate_at(child, depth + 1, budget))
                .collect::<Result<Vec<_>>>()?;
            intermediate["children"] = Value::Array(children);
        } else {
            intermediate["relation"] = as_reference(&origin["relation"])?;
        }
        Ok(intermediate)
    }

    /// Emit definitions at first use, including when a definition's original
    /// owner was removed or a new consumer now precedes it.
    pub(crate) fn relocate(&self, operators: &mut [Value]) -> Result<()> {
        let mut encoder = Encoder {
            definitions: self,
            origins: HashMap::new(),
            relations: HashMap::new(),
        };
        encoder.operators(operators, 0)
    }
}

fn empty(origin: &Value) -> bool {
    origin["kind"] == "CalciteEmptyRel"
        || origin["kind"] == "PositionOnlyRel" && origin["empty"] == true
}

fn sequence_parts(origin: &Value) -> Result<&[Value]> {
    match origin["kind"].as_str() {
        Some("RelSequence") => children(origin),
        Some("LastRel" | "IntermediateRel") => Ok(std::slice::from_ref(origin)),
        _ => Err(Error("unsupported chain provenance sequence".into())),
    }
}

struct Encoder<'a, 'b> {
    definitions: &'a Provenance<'b>,
    origins: HashMap<u32, usize>,
    relations: HashMap<u32, usize>,
}

impl Encoder<'_, '_> {
    fn operators(&mut self, operators: &mut [Value], depth: usize) -> Result<()> {
        check_nesting(depth)?;
        for operator in operators {
            if let Some(origin) = operator.get_mut("provenance") {
                *origin = self.origin(origin, 0)?;
            }
            if let Some(children) = operator.get_mut("allOperators") {
                self.operators(
                    children
                        .as_array_mut()
                        .ok_or_else(|| Error("invalid nested provenance scope".into()))?,
                    depth + 1,
                )?;
            }
        }
        Ok(())
    }

    fn origin(&mut self, wire: &Value, depth: usize) -> Result<Value> {
        check_nesting(depth)?;
        let old = identity(wire)?;
        if let Some(new) = self.origins.get(&old) {
            return Ok(json!({"ref": new}));
        }
        let new = self.origins.len();
        self.origins.insert(old, new);
        let mut result = resolve(&self.definitions.origins, wire)?.clone();
        result["id"] = new.into();
        if let Some(relation) = result.get_mut("relation") {
            let old = identity(relation)?;
            *relation = if let Some(new) = self.relations.get(&old) {
                json!({"ref": new})
            } else {
                let mut definition = resolve(&self.definitions.relations, relation)?.clone();
                let new = self.relations.len();
                self.relations.insert(old, new);
                definition["id"] = new.into();
                definition
            };
        }
        if let Some(children) = result.get_mut("children") {
            let children = children
                .as_array_mut()
                .ok_or_else(|| Error("invalid replay provenance children".into()))?;
            for child in children {
                *child = self.origin(child, depth + 1)?;
            }
        }
        Ok(result)
    }
}

fn check_nesting(depth: usize) -> Result<()> {
    if depth > MAX_NESTING {
        return Err(Error("replay provenance nesting exceeds 128".into()));
    }
    Ok(())
}

fn children(wire: &Value) -> Result<&[Value]> {
    wire["children"]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error("invalid replay provenance children".into()))
}

fn id(wire: &Value) -> Result<u32> {
    wire.as_u64()
        .and_then(|value| i32::try_from(value).ok())
        .map(|value| value as u32)
        .ok_or_else(|| Error("invalid replay provenance identity".into()))
}

fn identity(wire: &Value) -> Result<u32> {
    id(wire.get("ref").unwrap_or(&wire["id"]))
}

fn as_reference(wire: &Value) -> Result<Value> {
    Ok(json!({"ref": identity(wire)?}))
}

fn lookup<'a>(table: &HashMap<u32, &'a Value>, key: u32) -> Result<&'a Value> {
    table
        .get(&key)
        .copied()
        .ok_or_else(|| Error("undefined or forward replay provenance reference".into()))
}

fn resolve<'a>(table: &HashMap<u32, &'a Value>, wire: &'a Value) -> Result<&'a Value> {
    if let Some(reference) = wire.get("ref") {
        lookup(table, id(reference)?)
    } else {
        Ok(wire)
    }
}

fn insert<'a>(table: &mut HashMap<u32, &'a Value>, wire: &'a Value) -> Result<()> {
    if table.insert(id(&wire["id"])?, wire).is_some() {
        return Err(Error("duplicate replay provenance definition".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relocation_bounds_the_depth_of_expanded_references() {
        let mut operators = vec![json!({"provenance": {"id": 0, "kind": "PositionOnlyRel"}})];
        for previous in 0..=MAX_NESTING {
            operators.push(json!({"provenance": {
                "id": previous + 1, "kind": "RelSequence", "children": [{"ref": previous}],
            }}));
        }
        let before_pruning = Provenance::read(&operators).unwrap();
        let mut surviving = [operators.last().unwrap().clone()];
        assert!(before_pruning
            .relocate(&mut surviving)
            .unwrap_err()
            .to_string()
            .contains("nesting"));
    }

    #[test]
    fn intermediate_sequence_counts_repeated_shared_origins_against_its_budget() {
        let leaf = json!({"id": 0, "kind": "LastRel", "relation": {"id": 0}});
        let base = json!({"id": 1, "kind": "RelSequence", "children": [leaf]});
        let shared = json!({"ref": 1});
        let root = json!({"id": 2, "kind": "RelSequence", "children": [shared, shared]});
        let operators = [json!({"provenance": base}), json!({"provenance": root})];
        let mut origins = Provenance::read(&operators).unwrap();
        let error = origins
            .intermediate_at(&operators[1]["provenance"], 0, &mut 4)
            .unwrap_err();
        assert!(error.to_string().contains("expansion budget"));
    }
}
