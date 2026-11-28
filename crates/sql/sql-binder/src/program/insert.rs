//! Bind a literal INSERT source independently of its destination. Calcite
//! widens integer literals to their VALUES column type, but retains the
//! individual widths of character and binary literals and the type of NULL.

mod value;

use super::{
    StatementText, append_boundary,
    graph::{append_literal, append_project, append_values, insert_row_fields, insert_type},
    literal::{Cell, CellText},
    source::DeclarationTokens,
};
use crate::{Failure, SetExpr, local_name, query_body, unsupported};
use feldera_sql_rel_model::{
    catalog::Name,
    literal_insert,
    literals::{DecimalRounding, LiteralValue},
    program::Program,
    source::Origin,
    statements::{Statement, TargetColumn},
    types::{IntegerWidth, Precision, SqlType, TypeKind},
};
use sqlparser_064::ast::{Insert, TableObject};
use value::InsertValue;

pub(super) fn bind(
    program: &mut Program,
    insert: &Insert,
    tokens: &DeclarationTokens<'_>,
) -> Result<(), Failure> {
    let TableObject::TableName(target) = &insert.table else {
        return Err(unsupported("expected a plain INSERT target"));
    };
    let name = Name {
        name: local_name(target)?.value.to_lowercase(),
        quoted: false,
    };
    let columns = insert
        .columns
        .iter()
        .map(|column| {
            let ident = local_name(column)?;
            let name = ident.value.to_lowercase();
            Ok(TargetColumn {
                display_name: name.clone(),
                name: Name {
                    name,
                    quoted: false,
                },
                position: tokens.coordinates(ident.span)?,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    let query = insert
        .source
        .as_deref()
        .ok_or_else(|| unsupported("missing INSERT source"))?;
    let SetExpr::Values(values) = query_body(query)? else {
        return Err(unsupported("expected INSERT INTO ... VALUES"));
    };
    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        rows.push(
            row.iter()
                .map(InsertValue::parse)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    let literals: Option<Vec<Vec<&Cell>>> = rows
        .iter()
        .map(|row| row.iter().map(InsertValue::literal).collect())
        .collect();
    let relation = if let Some(literals) = literals {
        bind_values(program, &literals)?
    } else {
        let [cells] = rows.as_slice() else {
            return Err(unsupported(
                "multirow INSERT with projected values requires a union",
            ));
        };
        let mut fields = Vec::with_capacity(cells.len());
        let mut roots = Vec::with_capacity(cells.len());
        for (i, (cell, expr)) in cells.iter().zip(values.rows[0].iter()).enumerate() {
            let (root, ty) = cell.emit(program, tokens, expr)?;
            fields.push((format!("EXPR${i}"), ty));
            roots.push(root);
        }
        let row = insert_row_fields(
            program,
            fields.iter().map(|(name, ty)| (name.as_str(), *ty)),
        );
        let relation = program.table.relations;
        append_project(program, row, roots)?;
        relation
    };
    let columns = (!columns.is_empty()).then_some(columns);
    literal_insert::validate(
        program,
        &program.statements,
        &name,
        columns.as_deref(),
        relation,
    )
    .map_err(unsupported)?;
    let render = |quoted: bool| {
        let ident = |text: &str| {
            if quoted {
                format!("`{text}`")
            } else {
                text.to_owned()
            }
        };
        let mut header = format!("INSERT INTO {}", ident(&name.name));
        if let Some(columns) = &columns {
            let names = columns
                .iter()
                .map(|c| ident(&c.name.name))
                .collect::<Vec<_>>();
            header.push_str(&format!(" ({})", names.join(", ")));
        }
        let prefix = if quoted { "ROW" } else { "" };
        let rows = rows
            .iter()
            .map(|row| {
                let fields = row
                    .iter()
                    .map(|cell| cell.render(quoted))
                    .collect::<Vec<_>>();
                format!("{prefix}({})", fields.join(", "))
            })
            .collect::<Vec<_>>();
        format!("{header}\nVALUES {}", rows.join(",\n"))
    };
    let text = StatementText {
        position: tokens.range()?,
        pretty: render(true),
        diagnostic: render(false),
    };
    append_boundary(program, &text);
    program.producers.roots.push(relation);
    program.statements.push(Statement::ModifyTable {
        source: text.source("INSERT"),
        name,
        insert: true,
        relation: Some(relation),
        columns,
    });
    Ok(())
}

fn bind_values(program: &mut Program, rows: &[Vec<&Cell>]) -> Result<u32, Failure> {
    let width = rows.first().map_or(0, Vec::len);
    if width == 0 || rows.iter().any(|row| row.len() != width) {
        return Err(unsupported(
            "INSERT VALUES rows must have the same positive width",
        ));
    }
    let mut columns = Vec::with_capacity(width);
    for i in 0..width {
        let mut kind = TypeKind::Null {};
        let mut nullable = false;
        for row in rows {
            let cell = &row[i];
            nullable |= matches!(cell.value, LiteralValue::Null {});
            widen(&mut kind, &cell.kind)?;
        }
        let ty = insert_type(
            &mut program.table.type_table,
            SqlType {
                kind,
                nullable,
                origin: Origin::Absent {},
            },
        );
        columns.push((format!("EXPR${i}"), ty));
    }
    let row = insert_row_fields(
        program,
        columns.iter().map(|(name, ty)| (name.as_str(), *ty)),
    );
    // A CAST makes Calcite convert VALUES rows to projections before
    // reducing them back to literals. All cells inherit their column's
    // common type, including bare NULL and fields in other columns.
    let has_cast = rows
        .iter()
        .flatten()
        .any(|cell| matches!(cell.text, CellText::CastNull(_)));
    let mut roots = Vec::new();
    for cells in rows {
        for (cell, (_, column_type)) in cells.iter().zip(&columns) {
            let kind = if has_cast
                || matches!(
                    cell.kind,
                    TypeKind::Integer { .. } | TypeKind::Decimal { .. }
                ) {
                &program.table.type_table.types[column_type.0 as usize].kind
            } else {
                &cell.kind
            };
            let is_null = matches!(cell.value, LiteralValue::Null {});
            if !is_null
                && matches!(kind, TypeKind::Integer { signed: false, .. })
                && matches!(cell.kind, TypeKind::Integer { signed: true, .. })
            {
                return Err(unsupported(
                    "signed VALUES literal requires a projected unsigned cast",
                ));
            }
            let value = if let TypeKind::Decimal { scale, .. } = kind
                && !is_null
            {
                let decimal = cell
                    .value
                    .as_exact_decimal()
                    .ok_or_else(|| unsupported("non-numeric decimal VALUES element"))?;
                LiteralValue::Decimal {
                    value: decimal
                        .at_sql_scale(*scale, DecimalRounding::HalfUp)
                        .ok_or_else(|| {
                            unsupported("decimal VALUES element exceeds its common type")
                        })?,
                }
            } else {
                cell.value.clone()
            };
            let ty = SqlType {
                kind: kind.clone(),
                nullable: is_null,
                origin: Origin::Absent {},
            };
            let ty = insert_type(&mut program.table.type_table, ty);
            roots.push(append_literal(program, ty, value));
        }
    }
    append_values(program, row, rows.len() as u32, roots)
}

fn widen(kind: &mut TypeKind, next: &TypeKind) -> Result<(), Failure> {
    if *kind == *next || matches!(next, TypeKind::Null {}) {
        return Ok(());
    }
    match (kind, next) {
        (current @ TypeKind::Null {}, next) => *current = next.clone(),
        (
            TypeKind::Integer { width, signed },
            TypeKind::Integer {
                width: other,
                signed: rhs,
            },
        ) => {
            // Calcite keeps the first spelling at equal width, even when
            // signedness differs. Otherwise the wider operand supplies both.
            if width.bits() < other.bits() {
                *width = *other;
                *signed = *rhs;
            }
        }
        (
            TypeKind::Time {
                precision: Precision::Specified(current),
                zone: left,
            },
            TypeKind::Time {
                precision: Precision::Specified(next),
                zone: right,
            },
        )
        | (
            TypeKind::Timestamp {
                precision: Precision::Specified(current),
                zone: left,
            },
            TypeKind::Timestamp {
                precision: Precision::Specified(next),
                zone: right,
            },
        ) if left == right => *current = (*current).max(*next),
        (current, next)
            if let (Some((p1, s1)), Some((p2, s2))) =
                (decimal_shape(current), decimal_shape(next)) =>
        {
            let integral = (p1 - s1).max(p2 - s2);
            let scale = s1.max(s2).min(38 - integral);
            *current = TypeKind::Decimal {
                precision: integral + scale,
                scale,
                explicit_scale: true,
            };
        }
        (
            TypeKind::Character {
                fixed,
                length: width,
                ..
            },
            TypeKind::Character {
                fixed: other_fixed,
                length: other,
                ..
            },
        )
        | (
            TypeKind::Binary {
                fixed,
                length: width,
            },
            TypeKind::Binary {
                fixed: other_fixed,
                length: other,
                ..
            },
        ) => {
            *fixed &= *other_fixed && width == other;
            *width = match (*width, *other) {
                (Precision::Specified(a), Precision::Specified(b)) => {
                    Precision::Specified(a.max(b))
                }
                _ => Precision::Unspecified,
            };
        }
        _ => return Err(unsupported("incompatible INSERT VALUES column types")),
    }
    Ok(())
}

fn decimal_shape(kind: &TypeKind) -> Option<(u32, u32)> {
    match kind {
        TypeKind::Decimal {
            precision, scale, ..
        } => Some((*precision, *scale)),
        TypeKind::Integer {
            width: IntegerWidth::I8,
            ..
        } => Some((3, 0)),
        TypeKind::Integer {
            width: IntegerWidth::I16,
            ..
        } => Some((5, 0)),
        TypeKind::Integer {
            width: IntegerWidth::I32,
            ..
        } => Some((10, 0)),
        TypeKind::Integer {
            width: IntegerWidth::I64,
            ..
        } => Some((19, 0)),
        _ => None,
    }
}
