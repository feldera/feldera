//! Direct Rust formulation of the scalar rules, independent of the translated
//! strategy's family/checker/return-transform composition.
use crate::model::{Kind, Outcome, SqlType};

pub(crate) fn number(t: SqlType) -> Option<SqlType> {
    if t.numeric() {
        Some(t)
    } else if t.character() {
        Some(t.with_kind(Kind::Decimal {
            precision: 38,
            scale: 19,
        }))
    } else {
        None
    }
}

fn text(t: SqlType) -> SqlType {
    if t.character() {
        t
    } else {
        t.with_kind(Kind::Character {
            fixed: false,
            length: None,
        })
    }
}

fn varying(t: SqlType) -> SqlType {
    let Kind::Character { length, .. } = t.kind else {
        unreachable!("text operand")
    };
    t.with_kind(Kind::Character {
        fixed: false,
        length,
    })
}

fn exact(t: SqlType) -> Option<SqlType> {
    let t = number(t)?;
    Some(t.with_kind(match t.kind {
        Kind::Real => Kind::Decimal {
            precision: 14,
            scale: 7,
        },
        Kind::Double => Kind::Decimal {
            precision: 30,
            scale: 15,
        },
        kind => kind,
    }))
}

pub(crate) fn digits(kind: Kind) -> (u8, u8) {
    match kind {
        Kind::Integer { bits: 8 } => (3, 0),
        Kind::Integer { bits: 16 } => (5, 0),
        Kind::Integer { bits: 32 } => (10, 0),
        Kind::Integer { bits: 64 } => (19, 0),
        Kind::Decimal { precision, scale } => (precision, scale),
        _ => unreachable!("validated exact numeric type"),
    }
}

pub(crate) fn merge(a: SqlType, b: SqlType) -> Option<Kind> {
    use Kind::*;
    Some(match (a.kind, b.kind) {
        (x, y) if x == y => x,
        (
            Character {
                fixed: af,
                length: al,
            },
            Character {
                fixed: bf,
                length: bl,
            },
        ) => Character {
            fixed: af && bf && al == bl,
            length: al.zip(bl).map(|(a, b)| a.max(b)),
        },
        (Integer { bits: a }, Integer { bits: b }) => Integer { bits: a.max(b) },
        (Double, _) | (_, Double) if a.numeric() && b.numeric() => Double,
        (Real, Decimal { .. }) | (Decimal { .. }, Real) => Double,
        (Real, _) | (_, Real) if a.numeric() && b.numeric() => Real,
        (x, y) if a.numeric() && b.numeric() => {
            let (ap, as_) = digits(x);
            let (bp, bs) = digits(y);
            let whole = (ap - as_).max(bp - bs);
            let scale = as_.max(bs).min(38 - whole);
            Decimal {
                precision: whole + scale,
                scale,
            }
        }
        (Date, Timestamp { precision }) | (Timestamp { precision }, Date) => {
            Timestamp { precision }
        }
        (Character { .. }, _) | (_, Character { .. }) => Character {
            fixed: false,
            length: None,
        },
        _ => return None,
    })
}

fn unary(name: &str, input: SqlType) -> Option<(SqlType, SqlType)> {
    let (result, operand) = match name {
        "ABS" | "SIGN" | "ROUND" | "TRUNCATE" => {
            let operand = number(input)?;
            (operand, operand)
        }
        "CEIL" | "FLOOR" => {
            let operand = number(input)?;
            let kind = match operand.kind {
                Kind::Decimal { precision, .. } => Kind::Decimal {
                    precision,
                    scale: 0,
                },
                kind => kind,
            };
            (operand.with_kind(kind), operand)
        }
        "SQRT" | "CBRT" | "EXP" | "LN" | "LOG10" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS"
        | "ATAN" | "COT" | "DEGREES" | "RADIANS" => {
            let operand = number(input)?;
            (operand.with_kind(Kind::Double), operand)
        }
        "LENGTH" | "CHAR_LENGTH" | "ASCII" => {
            let operand = text(input);
            (operand.with_kind(Kind::Integer { bits: 32 }), operand)
        }
        "OCTET_LENGTH" => {
            let operand = match input.kind {
                Kind::Character { .. } => input.with_kind(Kind::Binary {
                    fixed: false,
                    length: None,
                }),
                Kind::Binary { .. } => input,
                _ => return None,
            };
            (operand.with_kind(Kind::Integer { bits: 32 }), operand)
        }
        "UPPER" | "LOWER" | "INITCAP" => (text(input), text(input)),
        "TRIM" | "REVERSE" => (varying(text(input)), text(input)),
        "MD5" => (
            input.with_kind(Kind::Character {
                fixed: false,
                length: None,
            }),
            text(input),
        ),
        _ => return None,
    };
    Some((result, operand))
}

fn binary(name: &str, a: SqlType, b: SqlType) -> Option<(SqlType, [SqlType; 2])> {
    let nullable = a.nullable || b.nullable;
    let (kind, operands) = match name {
        "POWER" | "LOG" | "ATAN2" => (Kind::Double, [number(a)?, number(b)?]),
        "MOD" => {
            let (a, b) = (exact(a)?, exact(b)?);
            let (ap, as_) = digits(a.kind);
            let (bp, bs) = digits(b.kind);
            let kind = if as_ == 0 && bs == 0 {
                b.kind
            } else {
                let scale = as_.max(bs);
                Kind::Decimal {
                    precision: (ap - as_).min(bp - bs) + scale,
                    scale,
                }
            };
            (kind, [a, b])
        }
        "LEFT" | "RIGHT" | "REPEAT" => {
            let b = match b.kind {
                Kind::Integer { .. } => b,
                Kind::Character { .. } => b.with_kind(Kind::Integer { bits: 64 }),
                _ => return None,
            };
            let a = text(a);
            let kind = if name == "REPEAT" {
                Kind::Character {
                    fixed: false,
                    length: None,
                }
            } else {
                varying(a).kind
            };
            (kind, [a, b])
        }
        "CONCAT" => {
            let (a, b) = (text(a), text(b));
            let (Kind::Character { length: al, .. }, Kind::Character { length: bl, .. }) =
                (a.kind, b.kind)
            else {
                unreachable!("text operands")
            };
            let length = al
                .zip(bl)
                .and_then(|(a, b)| a.checked_add(b))
                .filter(|n| *n <= i32::MAX as u32);
            (
                Kind::Character {
                    fixed: false,
                    length,
                },
                [a, b],
            )
        }
        "COALESCE" => {
            let kind = merge(a, b)?;
            let operands = match (a.character(), b.character()) {
                (true, false) => [a, b.with_kind(kind)],
                (false, true) => [a.with_kind(kind), b],
                _ => [a, b],
            };
            return Some((
                SqlType {
                    kind,
                    nullable: a.nullable && b.nullable,
                },
                operands,
            ));
        }
        "NULLIF" => {
            merge(a, b)?;
            return Some((
                SqlType {
                    nullable: true,
                    ..a
                },
                [a, b],
            ));
        }
        _ => return None,
    };
    Some((SqlType { kind, nullable }, operands))
}

/// Infer a declared scalar call without consulting the other strategy or Java.
pub fn infer(name: &str, arguments: &[SqlType]) -> Outcome {
    if crate::sampled_arity(name).is_some_and(|arity| arity != arguments.len()) {
        return Outcome::Unsupported {
            reason: "arity outside sampled function signature",
        };
    }
    if arguments
        .iter()
        .any(|t| matches!(t.kind, Kind::Binary { .. }))
    {
        return Outcome::Unsupported {
            reason: "binary input outside declared probe grammar",
        };
    }
    if arguments.iter().any(|t| !t.is_valid()) {
        return Outcome::Unsupported {
            reason: "invalid scalar type descriptor",
        };
    }
    if !crate::FUNCTIONS.contains(&name) {
        return Outcome::Unsupported {
            reason: "operation outside scalar function slice",
        };
    }
    match arguments {
        [a] => match unary(name, *a) {
            Some((result, operand)) => Outcome::bound(result, vec![operand]),
            None => Outcome::rejected(),
        },
        [a, b]
            if matches!(name, "COALESCE" | "NULLIF")
                && matches!(
                    (a.kind, b.kind),
                    (Kind::Date, Kind::Timestamp { .. }) | (Kind::Timestamp { .. }, Kind::Date)
                ) =>
        {
            Outcome::rejected()
        }
        [a, b] => match binary(name, *a, *b) {
            Some((result, operands)) => Outcome::bound(result, operands.to_vec()),
            None => Outcome::rejected(),
        },
        _ => Outcome::rejected(),
    }
}
