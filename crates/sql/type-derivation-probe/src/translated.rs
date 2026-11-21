// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Changed for Feldera: translated the bounded scalar branches of Calcite
// 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b into Rust. See README.md for
// method-level provenance and intentionally omitted binding context.

use crate::model::{Kind, Outcome, SqlType};

#[derive(Clone, Copy)]
enum Family {
    Numeric,
    ExactNumeric,
    Integer,
    Character,
    Binary,
}

#[derive(Clone, Copy)]
enum ReturnRule {
    First,
    NoScale,
    Double,
    Integer,
    Varying,
    Varchar,
    Concat,
    Mod,
}

fn signature(name: &str) -> Option<(&'static [Family], ReturnRule)> {
    use Family::*;
    use ReturnRule as R;
    Some(match name {
        "ABS" | "SIGN" | "ROUND" | "TRUNCATE" => (&[Numeric], R::First),
        "CEIL" | "FLOOR" => (&[Numeric], R::NoScale),
        "SQRT" | "CBRT" | "EXP" | "LN" | "LOG10" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS"
        | "ATAN" | "COT" | "DEGREES" | "RADIANS" => (&[Numeric], R::Double),
        "POWER" | "LOG" | "ATAN2" => (&[Numeric, Numeric], R::Double),
        "MOD" => (&[ExactNumeric, ExactNumeric], R::Mod),
        "LENGTH" | "CHAR_LENGTH" | "ASCII" => (&[Character], R::Integer),
        "OCTET_LENGTH" => (&[Binary], R::Integer),
        "UPPER" | "LOWER" | "INITCAP" => (&[Character], R::First),
        "TRIM" | "REVERSE" => (&[Character], R::Varying),
        "MD5" => (&[Character], R::Varchar),
        "CONCAT" => (&[Character, Character], R::Concat),
        "LEFT" | "RIGHT" => (&[Character, Integer], R::Varying),
        "REPEAT" => (&[Character, Integer], R::Varchar),
        _ => return None,
    })
}

/// AbstractTypeCoercion.implicitCast and family default concrete types.
fn implicit_cast(input: SqlType, family: Family) -> Option<SqlType> {
    use Family::*;
    let contains = match family {
        Numeric => input.numeric(),
        ExactNumeric => matches!(input.kind, Kind::Integer { .. } | Kind::Decimal { .. }),
        Integer => matches!(input.kind, Kind::Integer { .. }),
        Character => input.character(),
        Binary => matches!(input.kind, Kind::Binary { .. }),
    };
    if contains {
        return Some(input);
    }
    let kind = match (input.kind, family) {
        (Kind::Real, ExactNumeric) => Kind::Decimal {
            precision: 14,
            scale: 7,
        },
        (Kind::Double, ExactNumeric) => Kind::Decimal {
            precision: 30,
            scale: 15,
        },
        (Kind::Character { .. }, Numeric | ExactNumeric) => Kind::Decimal {
            precision: 38,
            scale: 19,
        },
        (Kind::Character { .. }, Integer) => Kind::Integer { bits: 64 },
        (Kind::Character { .. }, Binary) => Kind::Binary {
            fixed: false,
            length: None,
        },
        (_, Character) => Kind::Character {
            fixed: false,
            length: None,
        },
        _ => return None,
    };
    Some(input.with_kind(kind))
}

/// BasicSqlType's precision/scale, distinct from decimalOf(BIGINT).
fn exact_precision(kind: Kind) -> Option<(u8, u8)> {
    Some(match kind {
        Kind::Integer { bits: 8 } => (3, 0),
        Kind::Integer { bits: 16 } => (5, 0),
        Kind::Integer { bits: 32 } => (10, 0),
        Kind::Integer { bits: 64 } => (19, 0),
        Kind::Decimal { precision, scale } => (precision, scale),
        _ => return None,
    })
}

fn return_kind(rule: ReturnRule, operands: &[SqlType]) -> Kind {
    use ReturnRule::*;
    let first = operands[0].kind;
    match rule {
        First => first,
        NoScale => match first {
            Kind::Decimal { precision, .. } => Kind::Decimal {
                precision,
                scale: 0,
            },
            other => other,
        },
        Double => Kind::Double,
        Integer => Kind::Integer { bits: 32 },
        Varying => match first {
            Kind::Character { length, .. } => Kind::Character {
                fixed: false,
                length,
            },
            _ => unreachable!("signature requires character operand"),
        },
        Varchar => Kind::Character {
            fixed: false,
            length: None,
        },
        Concat => {
            let mut lengths = operands.iter().map(|t| match t.kind {
                Kind::Character { length, .. } => length,
                _ => unreachable!("signature requires character operands"),
            });
            let length = lengths
                .try_fold(0_u32, |sum, length| sum.checked_add(length?))
                .filter(|n| *n <= i32::MAX as u32);
            Kind::Character {
                fixed: false,
                length,
            }
        }
        Mod => {
            let second = operands[1].kind;
            if !matches!(first, Kind::Decimal { .. }) && !matches!(second, Kind::Decimal { .. }) {
                return second;
            }
            let (p1, s1) = exact_precision(first).expect("exact numeric signature");
            let (p2, s2) = exact_precision(second).expect("exact numeric signature");
            if s1 == 0 && s2 == 0 {
                return second;
            }
            let scale = s1.max(s2).min(38);
            Kind::Decimal {
                precision: ((p1 - s1).min(p2 - s2) + s1.max(s2)).min(38),
                scale,
            }
        }
    }
}

/// SqlTypeFactoryImpl.leastRestrictiveSqlType, followed by character promotion
/// from AbstractTypeCoercion.getWiderTypeForTwo for CASE/COALESCE.
fn common_type(a: SqlType, b: SqlType) -> Option<Kind> {
    let (x, y) = (a.kind, b.kind);
    if x == y {
        return Some(x);
    }
    if let (
        Kind::Character {
            fixed: fx,
            length: lx,
        },
        Kind::Character {
            fixed: fy,
            length: ly,
        },
    ) = (x, y)
    {
        return Some(Kind::Character {
            fixed: fx && fy && lx == ly,
            length: lx.zip(ly).map(|(x, y)| x.max(y)),
        });
    }
    if a.numeric() && b.numeric() {
        if let (Some((px, sx)), Some((py, sy))) = (exact_precision(x), exact_precision(y)) {
            if let (Kind::Integer { bits: bx }, Kind::Integer { bits: by }) = (x, y) {
                return Some(Kind::Integer { bits: bx.max(by) });
            }
            let whole = (px - sx).max(py - sy).min(38);
            let scale = sx.max(sy).min(38 - whole);
            return Some(Kind::Decimal {
                precision: whole + scale,
                scale,
            });
        }
        if matches!(x, Kind::Double | Kind::Decimal { .. })
            || matches!(y, Kind::Double | Kind::Decimal { .. })
        {
            return Some(Kind::Double);
        }
        return Some(Kind::Real);
    }
    if matches!(
        (x, y),
        (Kind::Date, Kind::Timestamp { .. }) | (Kind::Timestamp { .. }, Kind::Date)
    ) {
        return Some(Kind::Timestamp { precision: 6 });
    }
    if a.character() || b.character() {
        return Some(Kind::Character {
            fixed: false,
            length: None,
        });
    }
    None
}

/// Derive the validator's scalar result and family-coerced operand types.
/// Unsupported operations remain separate from ordinary SQL rejection.
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
    if name == "COALESCE" || name == "NULLIF" {
        let [a, b] = arguments else {
            return Outcome::rejected();
        };
        // SAME_VARIADIC / COMPARABLE_UNORDERED reject different datetime
        // families before CASE-style return inference can widen them.
        if matches!(
            (a.kind, b.kind),
            (Kind::Date, Kind::Timestamp { .. }) | (Kind::Timestamp { .. }, Kind::Date)
        ) {
            return Outcome::rejected();
        }
        let Some(common) = common_type(*a, *b) else {
            return Outcome::rejected();
        };
        if name == "NULLIF" {
            return Outcome::bound(
                SqlType {
                    nullable: true,
                    ..*a
                },
                arguments.to_vec(),
            );
        }
        return Outcome::bound(
            SqlType {
                kind: common,
                nullable: a.nullable && b.nullable,
            },
            // SqlCoalesceFunction.inferReturnType only invokes coercion if
            // least-restrictive inference failed. In this scalar domain that
            // is the character/noncharacter mix. needToCast preserves an
            // existing character operand when the target VARCHAR is unbounded.
            arguments
                .iter()
                .map(|t| {
                    if a.character() != b.character() && !t.character() {
                        t.with_kind(common)
                    } else {
                        *t
                    }
                })
                .collect(),
        );
    }
    let Some((families, rule)) = signature(name) else {
        return Outcome::Unsupported {
            reason: "operation outside scalar function slice",
        };
    };
    if families.len() != arguments.len() {
        return Outcome::rejected();
    }
    let Some(operands): Option<Vec<_>> = arguments
        .iter()
        .zip(families)
        .map(|(t, f)| implicit_cast(*t, *f))
        .collect()
    else {
        return Outcome::rejected();
    };
    let result = SqlType {
        kind: return_kind(rule, &operands),
        nullable: operands.iter().any(|t| t.nullable),
    };
    Outcome::bound(result, operands)
}
