//! Structured warnings for constructs that compare floating-point keys.

use super::{source::DeclarationTokens, source_origin};
use crate::{Failure, Select};
use feldera_sql_rel_model::{
    TypeId, TypeTable,
    catalog::Name,
    diagnostics::FrontendWarning,
    relation_metadata::Coordinates,
    types::{FloatFlavor, TypeKind},
};
use sqlparser_064::ast::Distinct;

fn floating_type(types: &TypeTable, ty: TypeId) -> Option<&'static str> {
    use std::collections::HashSet;

    // Multiple fields can reference the same type. Visit each definition once
    // even when nested map keys and values share their entire representation.
    let mut pending = vec![ty];
    let mut seen = HashSet::new();
    while let Some(next) = pending.pop() {
        if !seen.insert(next) {
            continue;
        }
        match &types.types[next.0 as usize].kind {
            TypeKind::Float { .. } => {
                return Some(match &types.types[ty.0 as usize].kind {
                    TypeKind::Float {
                        flavor: FloatFlavor::Real,
                    } => "REAL",
                    TypeKind::Float {
                        flavor: FloatFlavor::Float,
                    } => "FLOAT",
                    TypeKind::Float {
                        flavor: FloatFlavor::Double,
                    } => "DOUBLE",
                    TypeKind::Map { .. } => "MAP",
                    TypeKind::Array { .. } => "ARRAY",
                    _ => "ROW",
                });
            }
            TypeKind::Row { fields, .. } => pending.extend(fields.iter().map(|f| f.ty)),
            TypeKind::Array { element, .. } => pending.push(*element),
            TypeKind::Map { key, value } => pending.extend([key, value]),
            TypeKind::Named { representation, .. } => pending.push(*representation),
            _ => {}
        }
    }
    None
}

fn warning(
    tokens: &DeclarationTokens<'_>,
    position: Coordinates,
    construct: &str,
    subject: &str,
) -> Result<FrontendWarning, Failure> {
    Ok(FrontendWarning {
        context: source_origin(&tokens.range()?),
        range: source_origin(&position),
        category: "Floating point equality".into(),
        continuation: false,
        message: format!(
            "'{construct}' compares {subject} for equality.\nSee https://docs.feldera.com/sql/comparisons#comparing-floating-point-values"
        ),
    })
}

pub(super) fn key(
    types: &TypeTable,
    ty: TypeId,
    construct: &str,
    tokens: &DeclarationTokens<'_>,
    position: impl FnOnce() -> Result<Coordinates, Failure>,
) -> Result<Option<FrontendWarning>, Failure> {
    let Some(name) = floating_type(types, ty) else {
        return Ok(None);
    };
    let subject = match name {
        "ARRAY" | "ROW" | "MAP" => format!("{name} values containing floating point values"),
        _ => format!("floating point values of type {name}"),
    };
    warning(tokens, position()?, construct, &subject).map(Some)
}

pub(super) fn distinct(
    types: &TypeTable,
    select: &Select,
    fields: &[(Name, TypeId)],
    tokens: &DeclarationTokens<'_>,
) -> Result<Option<FrontendWarning>, Failure> {
    if !matches!(select.distinct, Some(Distinct::Distinct)) {
        return Ok(None);
    }
    let names = fields
        .iter()
        .filter(|(_, ty)| floating_type(types, *ty).is_some())
        .map(|(name, _)| format!("'{}'", name.name))
        .collect::<Vec<_>>();
    let subject = match names.as_slice() {
        [] => return Ok(None),
        [one] => format!("the floating point values in column {one}"),
        [first, second] => format!("the floating point values in columns {first} and {second}"),
        [rest @ .., last] => format!(
            "the floating point values in columns {}, and {last}",
            rest.join(", ")
        ),
    };
    warning(
        tokens,
        tokens.distinct_position(select.select_token.0.span)?,
        "DISTINCT",
        &subject,
    )
    .map(Some)
}
