//! Check literal INSERT assignments supported by the default pipeline compiler.
//! Both freshly bound SQL and detached S1 must resolve the destination in the
//! catalog at the statement, before lowering can issue the pipeline warning.

use crate::{
    catalog::Name,
    expression_objects::ObjectKind,
    literals::{DecimalRounding, LiteralValue, date_from_text},
    program::Program,
    relation_properties::Properties,
    statements::{Statement, TargetColumn},
    typed_literals::{decimal_fits, integer_fits},
    types::{SqlType, TypeKind},
};
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AssignmentWarning {
    pub kind: &'static str,
    pub message: String,
}

/// Check a complete positional INSERT or an explicit permutation. Assignment
/// casts outside this subset require the general DML translator. The caller
/// supplies a validated S1 graph or the binder's own arena references.
pub fn validate(
    program: &Program,
    preceding: &[Statement],
    target: &Name,
    columns: Option<&[TargetColumn]>,
    relation: u32,
) -> Result<Vec<AssignmentWarning>, String> {
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
    let types = &program.table.type_table.types;
    let props = &program.properties.relations[relation as usize];
    let Properties::Values { tuples } = props.fields else {
        return Err("INSERT source requires literal VALUES".into());
    };
    let producer = &program.producers.relations[relation as usize];
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
    // The compiler evaluates casts after permuting each row into table order.
    let mut order: Vec<_> = destinations.iter().enumerate().collect();
    order.sort_unstable_by_key(|(_, dest)| dest.index);
    let mut warnings = Vec::new();
    let mut reported = HashSet::new();
    for row in site.roots.chunks_exact(fields.len()) {
        for &(i, dest) in &order {
            let id = row[i];
            let ObjectKind::Literal { literal } =
                &program.expressions.expressions[id.0 as usize].kind
            else {
                return Err("expected a literal in INSERT VALUES".into());
            };
            if !fits_source(&literal.value, &types[fields[i].ty.0 as usize]) {
                return Err("literal does not fit the INSERT source row".into());
            }
            if let Some(warning) =
                assignment(&literal.value, &types[dest.ty.0 as usize], &dest.name.name)?
                && reported.insert(warning.clone())
            {
                warnings.push(warning);
            }
        }
    }
    Ok(warnings)
}

fn fits_source(value: &LiteralValue, ty: &SqlType) -> bool {
    match (value, &ty.kind) {
        (LiteralValue::Null {}, _) => ty.nullable,
        (LiteralValue::Boolean { .. }, TypeKind::Boolean {}) => true,
        (LiteralValue::Date { .. }, TypeKind::Date {}) => true,
        (
            LiteralValue::Decimal { value },
            TypeKind::Decimal {
                precision, scale, ..
            },
        ) => decimal_fits(value, *precision, *scale),
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

/// INSERT casts can truncate a string or leave a failed conversion for the
/// runtime. Pipeline definitions retain the conversion warnings.
fn assignment(
    value: &LiteralValue,
    target: &SqlType,
    column: &str,
) -> Result<Option<AssignmentWarning>, String> {
    let result = match (value, &target.kind) {
        (LiteralValue::Character { .. }, TypeKind::Character { .. })
        | (LiteralValue::Integer { .. } | LiteralValue::Decimal { .. }, TypeKind::Float { .. })
        | (LiteralValue::Decimal { .. }, TypeKind::Integer { .. }) => None,
        (LiteralValue::Character { value }, TypeKind::Date {}) => {
            let text = value.trim_matches(|c: char| c <= ' ');
            date_from_text(text).is_none().then(|| AssignmentWarning {
                kind: "Suspicious argument",
                message: format!("String '{value}' cannot be interpreted as a DATE"),
            })
        }
        (LiteralValue::Character { value }, TypeKind::Integer { width, signed }) => {
            let text = value.trim_matches(|c: char| c <= ' ');
            let fits = text.parse::<i64>().is_ok()
                && (*signed || !text.starts_with('-'))
                && integer_fits(text, *width, *signed);
            (!fits).then(|| AssignmentWarning {
                kind: "Not a number",
                message: format!(" String '{value}' cannot be interpreted as a number"),
            })
        }
        (
            _,
            TypeKind::Decimal {
                precision, scale, ..
            },
        ) if let Some(decimal) = value.as_exact_decimal() => {
            let fits = decimal
                .at_sql_scale(*scale, DecimalRounding::Truncate)
                .is_some_and(|v| decimal_fits(&v, *precision, *scale));
            if fits {
                None
            } else {
                let (_, spelling) = decimal
                    .sql_and_digest()
                    .ok_or("decimal assignment exceeds SQL limits")?;
                Some(AssignmentWarning {
                    kind: "Invalid DECIMAL",
                    message: format!(
                        "cannot represent {spelling} as DECIMAL({precision}, {scale}): precision of DECIMAL type too small to represent value"
                    ),
                })
            }
        }
        _ if fits_source(value, target) => None,
        _ => return Err(format!("unsupported INSERT assignment to {column}")),
    };
    Ok(result)
}
