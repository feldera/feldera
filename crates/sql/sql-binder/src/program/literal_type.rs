//! Common scalar types for VALUES columns and ARRAY elements.

use crate::{Failure, unsupported};
use feldera_sql_rel_model::types::{IntegerWidth, Precision, TypeKind};

pub(super) fn widen(kind: &mut TypeKind, next: &TypeKind) -> Result<(), Failure> {
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
        _ => return Err(unsupported("incompatible literal types")),
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
