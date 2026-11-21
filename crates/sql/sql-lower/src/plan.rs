//! Low-verbosity relational plan consumed by the profiler. View names are
//! ordered by Java string order. Inputs receive ids before their consumers,
//! and the same relation has one identity across view definitions.

use super::Error;
use feldera_sql_rel_model::{
    program::Program, relation_properties::Properties, statements::Statement, types::TypeKind,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};

/// Write the profiler's query plan for supported bound relational operators.
pub fn query_plan(program: &Program) -> Result<Value, Error> {
    let mut views: Vec<_> = program
        .statements
        .iter()
        .filter_map(|s| match s {
            Statement::CreateView { name, root, .. } => Some((&name.name, root.relation)),
            _ => None,
        })
        .collect();
    views.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
    let mut ids = HashMap::new();
    let mut plans = Map::new();
    for (view, root) in views {
        let mut emitted = Vec::new();
        let mut visiting = HashSet::new();
        let mut work = vec![(root, false)];
        while let Some((id, children_done)) = work.pop() {
            let relation = program
                .producers
                .relations
                .get(id as usize)
                .ok_or_else(|| Error(format!("undefined plan relation {id}")))?;
            if children_done {
                let plan_id = ids.len();
                let mut node = describe(program, id)?;
                node["id"] = plan_id.into();
                let inputs = relation
                    .inputs
                    .iter()
                    .map(|input| ids[input])
                    .collect::<Vec<usize>>();
                node["inputs"] = inputs.into();
                ids.insert(id, plan_id);
                emitted.push(node);
                visiting.remove(&id);
            } else if id == root || !ids.contains_key(&id) {
                if !visiting.insert(id) {
                    return Err(Error(format!("cycle in plan at relation {id}")));
                }
                work.push((id, true));
                work.extend(relation.inputs.iter().rev().map(|&child| (child, false)));
            }
        }
        plans.insert(view.clone(), json!({"rels":emitted}));
    }
    Ok(plans.into())
}

fn describe(program: &Program, id: u32) -> Result<Value, Error> {
    let relation = &program.producers.relations[id as usize];
    let properties = program
        .properties
        .relations
        .get(id as usize)
        .ok_or_else(|| Error("missing relation properties".into()))?;
    let ty = program
        .table
        .type_table
        .types
        .get(relation.row.0 as usize)
        .ok_or_else(|| Error("missing plan row type".into()))?;
    let TypeKind::Row { fields, .. } = &ty.kind else {
        return Err(Error("plan relation needs a row type".into()));
    };
    let mut node = json!({"id":null, "relOp":relation.form});
    match &properties.fields {
        Properties::Scan { name } => node["table"] = json!(name),
        Properties::Project { variables, .. } if variables.is_empty() => {
            node["fields"] = fields
                .iter()
                .map(|f| f.name.name.as_str())
                .collect::<Vec<_>>()
                .into();
        }
        Properties::Filter { variables } if variables.is_empty() => {}
        Properties::Values { .. } => {
            let mut columns = Vec::with_capacity(fields.len());
            for field in fields {
                let mut column = program
                    .table
                    .type_table
                    .types
                    .get(field.ty.0 as usize)
                    .and_then(|ty| ty.scalar_column_type())
                    .ok_or_else(|| Error("unsupported VALUES row type in profiler plan".into()))?;
                column["name"] = field.name.name.clone().into();
                columns.push(column);
            }
            node["type"] = columns.into();
        }
        Properties::Aggregate { group, groups, .. } if groups.as_slice() == [group.as_slice()] => {
            node["group"] = json!(group);
        }
        Properties::Set { all } => node["all"] = (*all).into(),
        _ => {
            return Err(Error(format!(
                "unsupported profiler plan attributes for {:?}",
                relation.form
            )))
        }
    }
    Ok(node)
}
