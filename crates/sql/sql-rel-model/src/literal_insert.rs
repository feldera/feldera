//! Check literal INSERT assignments supported by the default pipeline compiler.
//! Both freshly bound SQL and detached S1 must resolve the destination in the
//! catalog at the statement, before lowering can issue the pipeline warning.

use crate::{
    catalog::Name,
    expression_objects::ObjectKind,
    literals::LiteralValue,
    program::Program,
    relation_properties::Properties,
    statements::{Statement, TargetColumn},
    typed_literals::integer_fits,
    types::{SqlType, TypeKind},
};
use std::collections::HashSet;

/// Check a complete positional INSERT or an explicit permutation. Assignment
/// casts outside this subset require the general DML translator. The caller
/// supplies a validated S1 graph or the binder's own arena references.
pub fn validate(
    program: &Program,
    preceding: &[Statement],
    target: &Name,
    columns: Option<&[TargetColumn]>,
    relation: u32,
) -> Result<(), String> {
    let schema = preceding
        .iter()
        .find_map(|statement| match statement {
            Statement::CreateTable {
                source,
                name,
                schema,
                ..
            } if source.visible && name == target => program.catalog.schemas.get(*schema as usize),
            _ => None,
        })
        .ok_or_else(|| format!("INSERT target {} is not a declared table", target.name))?;
    let destinations: Vec<_> = match columns {
        None => schema.columns.iter().collect(),
        Some(columns) => {
            let mut seen = HashSet::new();
            if columns.len() != schema.columns.len() {
                return Err("INSERT requires all destination columns".into());
            }
            columns
                .iter()
                .map(|target| {
                    let column = schema
                        .columns
                        .iter()
                        .find(|column| column.name == target.name)
                        .ok_or_else(|| format!("unknown INSERT column {}", target.display_name))?;
                    if !seen.insert(column.index) {
                        return Err(format!("duplicate INSERT column {}", target.display_name));
                    }
                    Ok(column)
                })
                .collect::<Result<_, String>>()?
        }
    };
    let props = &program.properties.relations[relation as usize];
    let Properties::Values { tuples } = props.fields else {
        return Err("INSERT source requires literal VALUES".into());
    };
    let producer = &program.producers.relations[relation as usize];
    let types = &program.table.type_table.types;
    let TypeKind::Row { fields, .. } = &types[producer.row.0 as usize].kind else {
        return Err("INSERT source requires a row type".into());
    };
    if destinations.is_empty() || fields.len() != destinations.len() || tuples == 0 {
        return Err("INSERT column count differs from VALUES".into());
    }
    let mut sites = program
        .expressions
        .relation_uses
        .iter()
        .filter(|site| site.relation == relation);
    let site = sites.next().ok_or("missing INSERT source values")?;
    if sites.next().is_some() || Some(site.roots.len()) != fields.len().checked_mul(tuples as usize)
    {
        return Err("unexpected INSERT source shape".into());
    }
    for row in site.roots.chunks_exact(fields.len()) {
        for ((&id, field), dest) in row.iter().zip(fields).zip(&destinations) {
            let ObjectKind::Literal { literal } =
                &program.expressions.expressions[id.0 as usize].kind
            else {
                return Err("expected a literal in INSERT VALUES".into());
            };
            if !assignable(&literal.value, &types[field.ty.0 as usize]) {
                return Err("literal does not fit the INSERT source row".into());
            }
            if !assignable(&literal.value, &types[dest.ty.0 as usize]) {
                return Err(format!(
                    "unsupported INSERT assignment to {}",
                    dest.name.name
                ));
            }
        }
    }
    Ok(())
}

fn assignable(value: &LiteralValue, ty: &SqlType) -> bool {
    match (value, &ty.kind) {
        (LiteralValue::Null {}, _) => ty.nullable,
        (LiteralValue::Boolean { .. }, TypeKind::Boolean {}) => true,
        (LiteralValue::Integer { value }, TypeKind::Integer { width, signed }) => {
            integer_fits(value, *width, *signed)
        }
        (LiteralValue::Character { value }, TypeKind::Character { length, .. }) => {
            length.contains_length(value.encode_utf16().count())
        }
        (LiteralValue::Binary { value }, TypeKind::Binary { length, .. }) => {
            length.contains_length(value.len())
        }
        _ => false,
    }
}
