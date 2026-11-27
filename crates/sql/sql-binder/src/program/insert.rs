//! Bind a literal INSERT source independently of its destination. Calcite
//! widens integer literals to their VALUES column type, but retains the
//! individual widths of character and binary literals and the type of NULL.

use super::{
    StatementText, append_boundary,
    graph::{append_literal, append_values, insert_row_fields, insert_type},
    source::coordinates,
};
use crate::{Expr, Failure, SetExpr, Value, local_name, query_body, unsupported};
use feldera_sql_rel_model::{
    catalog::Name,
    literal_insert,
    literals::LiteralValue,
    program::Program,
    relation_metadata::Coordinates,
    source::Origin,
    statements::{Statement, TargetColumn},
    types::{CharacterMetadata, IntegerWidth, Precision, SqlType, TypeKind},
};
use sqlparser_064::ast::{Insert, TableObject, UnaryOperator};

pub(super) fn bind(
    program: &mut Program,
    insert: &Insert,
    position: Coordinates,
) -> Result<(), Failure> {
    let TableObject::TableName(target) = &insert.table else {
        return Err(unsupported("expected a plain INSERT target"));
    };
    let name = Name {
        name: local_name(target)?.value.to_ascii_lowercase(),
        quoted: false,
    };
    let columns = insert
        .columns
        .iter()
        .map(|column| {
            let ident = local_name(column)?;
            let name = ident.value.to_ascii_lowercase();
            Ok(TargetColumn {
                display_name: name.clone(),
                name: Name {
                    name,
                    quoted: false,
                },
                position: coordinates(ident.span),
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
        rows.push(row.iter().map(Cell::parse).collect::<Result<Vec<_>, _>>()?);
    }
    let relation = bind_values(program, &rows)?;
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
                let fields = row.iter().map(|cell| cell.sql.as_str()).collect::<Vec<_>>();
                format!("{prefix}({})", fields.join(", "))
            })
            .collect::<Vec<_>>();
        format!("{header}\nVALUES {}", rows.join(",\n"))
    };
    let text = StatementText {
        position,
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

struct Cell {
    value: LiteralValue,
    kind: TypeKind,
    sql: String,
}

impl Cell {
    fn parse(expr: &Expr) -> Result<Self, Failure> {
        let integer = |digits: &str, negative| {
            if !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(unsupported("expected an exact integral VALUES literal"));
            }
            let signed = if negative {
                format!("-{digits}")
            } else {
                digits.into()
            };
            let n: i64 = signed
                .parse()
                .map_err(|_| unsupported("VALUES integer exceeds BIGINT"))?;
            let width = if i32::try_from(n).is_ok() {
                IntegerWidth::I32
            } else {
                IntegerWidth::I64
            };
            let sql = n.to_string();
            Ok(Self {
                value: LiteralValue::Integer { value: sql.clone() },
                kind: TypeKind::Integer {
                    width,
                    signed: true,
                },
                sql,
            })
        };
        match expr {
            Expr::Nested(expr) => Self::parse(expr),
            Expr::UnaryOp {
                op: op @ (UnaryOperator::Plus | UnaryOperator::Minus),
                expr,
            } => {
                if let Expr::Value(lit) = expr.as_ref()
                    && let Value::Number(digits, false) = &lit.value
                {
                    return integer(digits, *op == UnaryOperator::Minus);
                }
                Err(unsupported("expected a signed integer in VALUES"))
            }
            Expr::Value(lit) => {
                let (value, kind, sql) = match &lit.value {
                    Value::Number(digits, false) => return integer(digits, false),
                    Value::Null => (LiteralValue::Null {}, TypeKind::Null {}, "NULL".into()),
                    Value::Boolean(v) => (
                        LiteralValue::Boolean { value: *v },
                        TypeKind::Boolean {},
                        if *v { "TRUE" } else { "FALSE" }.into(),
                    ),
                    Value::SingleQuotedString(text)
                        if text.bytes().all(|b| !b.is_ascii_control()) =>
                    {
                        (
                            LiteralValue::Character {
                                value: text.clone(),
                            },
                            TypeKind::Character {
                                fixed: true,
                                length: Precision::Specified(text.encode_utf16().count() as u32),
                                metadata: CharacterMetadata::implicit(),
                            },
                            format!("'{}'", text.replace('\'', "''")),
                        )
                    }
                    Value::HexStringLiteral(text)
                        if text.len().is_multiple_of(2)
                            && text.bytes().all(|c| c.is_ascii_hexdigit()) =>
                    {
                        let bytes = (0..text.len())
                            .step_by(2)
                            .map(|i| {
                                u8::from_str_radix(&text[i..i + 2], 16).expect("checked hex digits")
                            })
                            .collect::<Vec<_>>();
                        (
                            LiteralValue::Binary { value: bytes },
                            TypeKind::Binary {
                                fixed: true,
                                length: Precision::Specified((text.len() / 2) as u32),
                            },
                            format!("X'{}'", text.to_ascii_uppercase()),
                        )
                    }
                    _ => return Err(unsupported("unsupported INSERT literal")),
                };
                Ok(Self { value, kind, sql })
            }
            _ => Err(unsupported("expected a literal INSERT value")),
        }
    }
}

fn bind_values(program: &mut Program, rows: &[Vec<Cell>]) -> Result<u32, Failure> {
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
            nullable |= matches!(cell.kind, TypeKind::Null {});
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
    let mut roots = Vec::new();
    for cells in rows {
        for (cell, (_, column_type)) in cells.iter().zip(&columns) {
            let kind = match cell.kind {
                TypeKind::Integer { .. } => {
                    &program.table.type_table.types[column_type.0 as usize].kind
                }
                _ => &cell.kind,
            };
            let ty = SqlType {
                kind: kind.clone(),
                nullable: matches!(kind, TypeKind::Null {}),
                origin: Origin::Absent {},
            };
            let ty = insert_type(&mut program.table.type_table, ty);
            roots.push(append_literal(program, ty, cell.value.clone()));
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
        (TypeKind::Integer { width, .. }, TypeKind::Integer { .. }) => *width = IntegerWidth::I64,
        (
            TypeKind::Character {
                fixed,
                length: Precision::Specified(width),
                ..
            },
            TypeKind::Character {
                length: Precision::Specified(n),
                ..
            },
        )
        | (
            TypeKind::Binary {
                fixed,
                length: Precision::Specified(width),
            },
            TypeKind::Binary {
                length: Precision::Specified(n),
                ..
            },
        ) => {
            *fixed &= *width == *n;
            *width = (*width).max(*n);
        }
        _ => return Err(unsupported("incompatible INSERT VALUES column types")),
    }
    Ok(())
}
