//! Compact Calcite type names in LogicalValues digests. Record kinds and
//! precision are observable even when the collection has no rows.
use crate::Error;
use feldera_sql_rel_model::{
    types::{Cardinality, StructKind, TypeKind},
    TypeId, TypeTable, ValidationLimits,
};
use std::fmt::Write;

pub(crate) fn render(types: &TypeTable, id: TypeId) -> Result<String, Error> {
    let mut text = String::new();
    write_type(
        types,
        id,
        &mut text,
        &mut ValidationLimits::default().references,
    )?;
    Ok(text)
}

// The bound program has already passed depth and cycle validation. A shared
// type can still expand multiple times when rendered as a tree.
fn write_type(
    types: &TypeTable,
    id: TypeId,
    text: &mut String,
    budget: &mut usize,
) -> Result<(), Error> {
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| Error("expanded VALUES row type exceeds the reference limit".into()))?;
    let ty = &types.types[id.0 as usize];
    match &ty.kind {
        TypeKind::Row {
            fields,
            struct_kind,
        } => {
            text.push_str(match struct_kind {
                StructKind::FullyQualified => "RecordType(",
                StructKind::PeekFields => "RecordType:peek(",
                StructKind::PeekFieldsDefault => "RecordType:peek_default(",
                StructKind::PeekFieldsNoExpand => "RecordType:peek_no_expand(",
            });
            for (i, field) in fields.iter().enumerate() {
                if i != 0 {
                    text.push_str(", ");
                }
                write_type(types, field.ty, text, budget)?;
                text.push(' ');
                text.push_str(&field.name.name);
            }
            text.push(')');
        }
        TypeKind::Array {
            element,
            cardinality: Cardinality::Unbounded,
        } => {
            write_type(types, *element, text, budget)?;
            text.push_str(" ARRAY");
        }
        TypeKind::Map { key, value } => {
            text.push('(');
            write_type(types, *key, text, budget)?;
            text.push_str(", ");
            write_type(types, *value, text, budget)?;
            text.push_str(") MAP");
        }
        _ => {
            let descriptor = ty
                .scalar_column_type()
                .ok_or_else(|| Error("unsupported VALUES column type".into()))?;
            let name = descriptor["type"]
                .as_str()
                .expect("scalar descriptor has a SQL type");
            if matches!(ty.kind, TypeKind::Integer { signed: false, .. }) {
                text.push_str(name.strip_prefix('U').expect("unsigned SQL type"));
                text.push_str(" UNSIGNED");
            } else {
                text.push_str(name);
            }
            if let Some(precision) = descriptor["precision"].as_u64() {
                if let TypeKind::Decimal {
                    explicit_scale: true,
                    scale,
                    ..
                } = ty.kind
                {
                    write!(text, "({precision}, {scale})").expect("string write");
                } else {
                    write!(text, "({precision})").expect("string write");
                }
            }
        }
    }
    Ok(())
}
