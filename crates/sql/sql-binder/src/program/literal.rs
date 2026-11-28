//! SQL literals shared by VALUES and constant SELECT projections.

mod temporal;

use super::{graph, source::DeclarationTokens};
use crate::{Expr, Failure, Value, unsupported};
use feldera_sql_rel_model::{
    TypeId,
    expression_objects::ObjectPosition,
    expressions::ExprId,
    literals::{ExactDecimal, LiteralValue},
    operators::OperatorObject,
    program::Program,
    source::Origin,
    types::{CharacterMetadata, IntegerWidth, Precision, SqlType, TypeKind},
};
use sqlparser_064::ast::{CastKind, UnaryOperator};
use std::borrow::Cow;

pub(super) enum CellText {
    Literal(String),
    CastNull(super::table::TypeSpelling),
}

impl CellText {
    pub(super) fn render(&self, quoted: bool) -> Cow<'_, str> {
        match self {
            Self::Literal(text) => Cow::Borrowed(text),
            Self::CastNull(ty) => Cow::Owned(format!("CAST(NULL AS {})", ty.spelling(quoted))),
        }
    }
}

fn is_null_literal(mut expr: &Expr) -> bool {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    matches!(expr, Expr::Value(literal) if literal.value == Value::Null)
}

pub(super) struct Cell {
    pub(super) value: LiteralValue,
    pub(super) kind: TypeKind,
    pub(super) text: CellText,
}

impl Cell {
    pub(super) fn needs_non_bmp_wrapper(&self) -> bool {
        self.value.contains_supplementary_character()
    }

    pub(super) fn emit(
        &self,
        program: &mut Program,
        tokens: &DeclarationTokens<'_>,
        expr: &Expr,
    ) -> Result<(ExprId, TypeId), Failure> {
        let source = SqlType {
            nullable: matches!(self.value, LiteralValue::Null {}),
            kind: self.kind.clone(),
            origin: Origin::Absent {},
        };
        let wrapped = self.value.non_bmp_result_type(&source);
        let ty = graph::insert_type(&mut program.table.type_table, source);
        let literal = graph::append_literal(program, ty, self.value.clone());
        if let Some(result) = wrapped {
            let result = graph::insert_type(&mut program.table.type_table, result);
            Ok((
                graph::append_constant_call(
                    program,
                    OperatorObject::non_bmp_literal(),
                    vec![literal],
                    result,
                    ObjectPosition::Present {
                        position: tokens.literal_position(expr)?,
                        internal: None,
                    },
                ),
                result,
            ))
        } else {
            Ok((literal, ty))
        }
    }

    fn number(text: &str, negative: bool) -> Result<Self, Failure> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return Err(unsupported("expected an exact numeric VALUES literal"));
        }
        let sign = if negative { "-" } else { "" };
        if fraction.is_empty()
            && let Ok(n) = format!("{sign}{whole}").parse::<i64>()
        {
            let width = if i32::try_from(n).is_ok() {
                IntegerWidth::I32
            } else {
                IntegerWidth::I64
            };
            let sql = n.to_string();
            return Ok(Self {
                value: LiteralValue::Integer { value: sql.clone() },
                kind: TypeKind::Integer {
                    width,
                    signed: true,
                },
                text: CellText::Literal(sql),
            });
        }

        // Calcite counts the digits in the token, including leading zeros.
        // The coefficient itself has no leading zeros or negative zero.
        let precision = whole.len() + fraction.len();
        if precision > 38 {
            return Err(unsupported("exact numeric literal exceeds 38 digits"));
        }
        let scale = fraction.len() as u32;
        let coefficient = format!("{sign}{whole}{fraction}")
            .parse::<i128>()
            .map_err(|_| unsupported("invalid decimal coefficient"))?;
        let decimal = ExactDecimal {
            coefficient: coefficient.to_string(),
            scale: scale as i32,
        };
        let (sql, _) = decimal.sql_and_digest().expect("checked SQL decimal");
        Ok(Self {
            value: LiteralValue::Decimal { value: decimal },
            kind: TypeKind::Decimal {
                precision: precision as u32,
                scale,
                explicit_scale: true,
            },
            text: CellText::Literal(sql),
        })
    }

    pub(super) fn parse(expr: &Expr) -> Result<Self, Failure> {
        match expr {
            Expr::Nested(expr) => Self::parse(expr),
            Expr::Cast {
                kind: CastKind::Cast,
                expr,
                data_type,
                format: None,
            } if is_null_literal(expr) => {
                let (kind, ty) = super::table::null_cast_type(data_type)?;
                Ok(Self {
                    kind,
                    value: LiteralValue::Null {},
                    text: CellText::CastNull(ty),
                })
            }
            Expr::Identifier(ident)
                if ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("UNKNOWN") =>
            {
                Ok(Self {
                    kind: TypeKind::Boolean {},
                    value: LiteralValue::Null {},
                    text: CellText::Literal("UNKNOWN".into()),
                })
            }
            Expr::TypedString(typed)
                if typed.data_type
                    == crate::DataType::Decimal(sqlparser_064::ast::ExactNumberInfo::None) =>
            {
                let Value::SingleQuotedString(text) = &typed.value.value else {
                    return Err(unsupported("expected a quoted DECIMAL"));
                };
                Self::quoted_decimal(text)
                    .ok_or_else(|| unsupported("DECIMAL literal exceeds the supported precision"))
            }
            Expr::TypedString(typed) => {
                let Value::SingleQuotedString(text) = &typed.value.value else {
                    return Err(unsupported("expected a quoted typed literal"));
                };
                if typed.data_type == crate::DataType::Uuid {
                    let bytes = feldera_sql_rel_model::literals::uuid_from_text(text)
                        .ok_or_else(|| unsupported("invalid UUID literal"))?;
                    return Ok(Self {
                        value: LiteralValue::Uuid { bytes },
                        kind: TypeKind::Uuid {},
                        text: CellText::Literal(format!("UUID '{text}'")),
                    });
                }
                temporal::parse(&typed.data_type, text)
                    .ok_or_else(|| unsupported("unsupported typed literal"))
            }
            Expr::UnaryOp {
                op: op @ (UnaryOperator::Plus | UnaryOperator::Minus),
                expr,
            } => {
                if let Expr::Value(lit) = expr.as_ref()
                    && let Value::Number(digits, false) = &lit.value
                {
                    return Self::number(digits, *op == UnaryOperator::Minus);
                }
                Err(unsupported("expected a signed numeric literal in VALUES"))
            }
            Expr::Value(lit) => {
                let (value, kind, sql) = match &lit.value {
                    Value::Number(digits, false) => return Self::number(digits, false),
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
                Ok(Self {
                    value,
                    kind,
                    text: CellText::Literal(sql),
                })
            }
            _ => Err(unsupported("expected a literal VALUES element")),
        }
    }

    /// Calcite parses a typed DECIMAL string as BigDecimal before deriving
    /// its SQL type from the plain spelling. Leading zeros in SQL tokens
    /// contribute to precision, while leading zeros in this string do not.
    fn quoted_decimal(text: &str) -> Option<Self> {
        let text = text.trim_matches(' ');
        let (mantissa, power) = match text.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
            None => (text, 0),
        };
        let unsigned = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let scale = i32::try_from(fraction.len()).ok()?.checked_sub(power)?;
        let mut n: i128 = format!("{whole}{fraction}").parse().ok()?;
        if mantissa.starts_with('-') {
            n = -n;
        }
        if scale < 0 {
            n = n.checked_mul(10_i128.checked_pow(scale.unsigned_abs())?)?;
        }
        let decimal = ExactDecimal {
            coefficient: n.to_string(),
            scale: scale.max(0),
        };
        let (sql, _) = decimal.sql_and_digest()?;
        Self::number(sql.trim_start_matches('-'), n < 0).ok()
    }
}
