//! The S2 encoder numbers shared origins and relational records by their first
//! appearance in operator order. A table can acquire more scan origins after
//! its first view has been constructed.

use super::Error;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Default)]
struct Ids(HashMap<u64, usize>);

impl Ids {
    fn assign(&mut self, value: &mut Value) -> Result<(), Error> {
        if let Some(reference) = value.get_mut("ref") {
            let id = reference
                .as_u64()
                .and_then(|id| self.0.get(&id))
                .ok_or_else(|| Error("origin reference precedes its definition".into()))?;
            *reference = (*id).into();
        } else {
            let previous = value["id"]
                .as_u64()
                .ok_or_else(|| Error("missing origin identity".into()))?;
            let next = self.0.len();
            if self.0.insert(previous, next).is_some() {
                return Err(Error("duplicate origin definition".into()));
            }
            value["id"] = next.into();
        }
        Ok(())
    }
}

fn visit(origin: &mut Value, origins: &mut Ids, relations: &mut Ids) -> Result<(), Error> {
    origins.assign(origin)?;
    if let Some(relation) = origin.get_mut("relation") {
        relations.assign(relation)?;
    }
    if let Some(children) = origin.get_mut("children").and_then(Value::as_array_mut) {
        for child in children {
            visit(child, origins, relations)?;
        }
    }
    Ok(())
}

pub(super) fn assign(graph: &mut Value) -> Result<(), Error> {
    let mut origins = Ids::default();
    let mut relations = Ids::default();
    for op in graph["allOperators"]
        .as_array_mut()
        .expect("system circuit")
    {
        visit(&mut op["provenance"], &mut origins, &mut relations)?;
    }
    Ok(())
}
