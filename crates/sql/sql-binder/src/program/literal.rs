//! SQL literals shared by VALUES and constant SELECT projections.

use crate::{Expr, Failure, Value, unsupported};
use feldera_sql_rel_model::{
    literals::LiteralValue,
    types::{CharacterMetadata, IntegerWidth, Precision, TypeKind},
};
use sqlparser_064::ast::UnaryOperator;

pub(super) struct Cell {
    pub(super) value: LiteralValue,
    pub(super) kind: TypeKind,
    pub(super) sql: String,
}

impl Cell {
    pub(super) fn parse(expr: &Expr) -> Result<Self, Failure> {
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
                    _ => return Err(unsupported("unsupported SQL literal")),
                };
                Ok(Self { value, kind, sql })
            }
            _ => Err(unsupported("expected a literal VALUES element")),
        }
    }
}
