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
// Changed for Feldera: bounded Rust implementation of decimal arithmetic
// formulas from RelDataTypeSystem at the Calcite revision in README.md.
//! Additional spike-5 forms; these are not part of the two-strategy experiment.
use crate::{
    model::{DerivedType, Kind, Outcome, SqlType},
    rewritten::{digits, merge, number},
};

fn arithmetic(name: &str, mut a: SqlType, mut b: SqlType) -> Option<(Kind, [SqlType; 2])> {
    // Arithmetic first attempts string-to-peer coercion, unlike the generic
    // NUMERIC family checker used by scalar functions.
    if a.character() && b.numeric() {
        a = a.with_kind(if matches!(b.kind, Kind::Decimal { .. }) {
            Kind::Decimal {
                precision: 38,
                scale: 19,
            }
        } else {
            b.kind
        });
    } else if b.character() && a.numeric() {
        b = b.with_kind(if matches!(a.kind, Kind::Decimal { .. }) {
            Kind::Decimal {
                precision: 38,
                scale: 19,
            }
        } else {
            a.kind
        });
    }
    let (a, b) = (number(a)?, number(b)?);
    let exact = |t: SqlType| matches!(t.kind, Kind::Integer { .. } | Kind::Decimal { .. });
    let decimal = matches!(a.kind, Kind::Decimal { .. }) || matches!(b.kind, Kind::Decimal { .. });
    let kind = if exact(a) && exact(b) && decimal {
        let (p1, s1) = digits(a.kind);
        let (p2, s2) = digits(b.kind);
        let (precision, scale) = match name {
            "+" | "-" => {
                let scale = s1.max(s2);
                (((p1 - s1).max(p2 - s2) + scale + 1).min(38), scale)
            }
            "*" => ((p1 + p2).min(38), (s1 + s2).min(38)),
            "/" => {
                let whole = p1 - s1 + s2;
                let mut scale = 6.max(s1 + p2 + 1).min(38);
                let precision = whole + scale;
                scale = if precision <= 32 {
                    scale.min(38 - whole)
                } else {
                    scale.min(6)
                };
                (precision.min(38), scale)
            }
            _ => unreachable!("arithmetic operator"),
        };
        Kind::Decimal { precision, scale }
    } else {
        merge(a, b)?
    };
    Some((kind, [a, b]))
}

fn aggregate(name: &str, input: SqlType) -> Outcome {
    if name == "ARRAY_AGG" {
        return Outcome::bound(
            DerivedType::Array {
                component: Box::new(input.into()),
                nullable: input.nullable,
            },
            vec![input],
        );
    }
    if name == "COUNT" {
        return Outcome::bound(
            SqlType {
                kind: Kind::Integer { bits: 64 },
                nullable: false,
            },
            vec![input],
        );
    }
    let operand = if matches!(name, "SUM" | "AVG") {
        let Some(t) = number(input) else {
            return Outcome::rejected();
        };
        t
    } else {
        input
    };
    let kind = match (name, operand.kind) {
        ("SUM", Kind::Decimal { scale, .. }) => Kind::Decimal {
            precision: 38,
            scale,
        },
        (_, kind) => kind,
    };
    Outcome::bound(
        SqlType {
            kind,
            nullable: true,
        },
        vec![operand],
    )
}

/// Infer the global aggregates and binary forms emitted by sql-type-probes.
/// Grouped aggregates and arbitrary CASE predicates are outside this adapter.
pub fn infer(name: &str, arguments: &[SqlType]) -> Outcome {
    if arguments.iter().any(|t| !t.is_valid()) {
        return Outcome::Unsupported {
            reason: "invalid scalar type descriptor",
        };
    }
    if matches!(name, "SUM" | "AVG" | "MIN" | "MAX" | "COUNT" | "ARRAY_AGG") {
        return match arguments {
            [t] => aggregate(name, *t),
            _ => Outcome::rejected(),
        };
    }
    if !matches!(
        name,
        "+" | "-" | "*" | "/" | "=" | "<" | "CASE" | "UNION ALL" | "INTERSECT ALL" | "EXCEPT ALL"
    ) {
        return Outcome::Unsupported {
            reason: "operation outside additional forms",
        };
    }
    let [a, b] = arguments else {
        return Outcome::rejected();
    };
    let nullable = a.nullable || b.nullable;
    if matches!(name, "+" | "-" | "*" | "/") {
        return match arithmetic(name, *a, *b) {
            Some((kind, operands)) => Outcome::bound(SqlType { kind, nullable }, operands.to_vec()),
            None => Outcome::rejected(),
        };
    }
    if matches!(name, "=" | "<") {
        let boolean_numeric = (matches!(a.kind, Kind::Boolean) && b.numeric())
            || (matches!(b.kind, Kind::Boolean) && a.numeric());
        if merge(*a, *b).is_none() && !(name == "=" && boolean_numeric) {
            return Outcome::rejected();
        }
        // A result-only projection. The comparison operator's conversion tree
        // is deliberately not claimed by these original argument descriptors.
        return Outcome::bound(
            SqlType {
                kind: Kind::Boolean,
                nullable,
            },
            arguments.to_vec(),
        );
    }
    // Preserve Calcite's directional leastRestrictiveSqlType shortcut: an
    // exact numeric first operand looks ahead to datetime. In the reverse
    // order only integral types pass leastRestrictiveByCast's mapping rule.
    let common = match (a.kind, b.kind) {
        (
            Kind::Integer { .. } | Kind::Decimal { .. },
            datetime @ (Kind::Date | Kind::Timestamp { .. }),
        ) => Some(datetime),
        (datetime @ (Kind::Date | Kind::Timestamp { .. }), Kind::Integer { .. }) => Some(datetime),
        _ => merge(*a, *b),
    };
    let Some(kind) = common else {
        return Outcome::rejected();
    };
    // ReturnTypes.LEAST_RESTRICTIVE_INTERSECT/EXCEPT refine the common row's
    // nullability independently of its type widening.
    let nullable = match name {
        "INTERSECT ALL" => a.nullable && b.nullable,
        "EXCEPT ALL" => a.nullable,
        _ => nullable,
    };
    Outcome::bound(
        SqlType { kind, nullable },
        arguments.iter().map(|t| t.with_kind(kind)).collect(),
    )
}
