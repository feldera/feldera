//! Determine whether a scalar cast preserves a lower bound and which runtime cast to use.
use feldera_sql_ir::{CircuitEditor, Error, Node, TypeId, TypeKind};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CastWaterline {
    None,
    Identity,
    Ordinary,
    Saturating,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Domain {
    Integer { bits: u32, signed: bool },
    Decimal { integral_digits: u32, scale: u32 },
    Float,
    Date,
    Time,
    Timestamp,
    ShortInterval { single_unit: bool },
    LongInterval { single_unit: bool },
    Bool,
    MachineInteger,
    Other,
}

impl Domain {
    fn read(edit: &CircuitEditor, ty: TypeId) -> Result<Self, Error> {
        let node = edit.node(ty.into())?;
        Ok(match edit.type_kind(ty)? {
            TypeKind::Integer => {
                let bits = attribute(node, "width")?;
                if !matches!(bits, 8 | 16 | 32 | 64 | 128) {
                    return Err(Error::new("unexpected integer width in waterline cast"));
                }
                let signed = node
                    .data("signed")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("missing integer signedness in waterline cast"))?;
                Self::Integer { bits, signed }
            }
            TypeKind::Decimal => {
                let precision = attribute(node, "precision")?;
                let scale = attribute(node, "scale")?;
                if precision > 38 || scale > precision {
                    return Err(Error::new(
                        "invalid decimal precision or scale in waterline cast",
                    ));
                }
                Self::Decimal {
                    integral_digits: precision - scale,
                    scale,
                }
            }
            TypeKind::Real | TypeKind::Double => Self::Float,
            TypeKind::Date => Self::Date,
            TypeKind::Time => Self::Time,
            TypeKind::Timestamp | TypeKind::TimestampTz => Self::Timestamp,
            TypeKind::ShortInterval | TypeKind::LongInterval => {
                let units = node
                    .data("units")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::new("missing interval units in waterline cast"))?;
                if edit.type_kind(ty)? == TypeKind::ShortInterval {
                    Self::ShortInterval {
                        single_unit: matches!(units, "DAYS" | "HOURS" | "MINUTES" | "SECONDS"),
                    }
                } else {
                    Self::LongInterval {
                        single_unit: matches!(units, "YEARS" | "MONTHS"),
                    }
                }
            }
            TypeKind::Bool => Self::Bool,
            TypeKind::ISize | TypeKind::USize => Self::MachineInteger,
            _ => Self::Other,
        })
    }

    /// Can this conversion represent every source value? The caller has already
    /// handled an exact type match, including types outside the numeric domains.
    fn fits_in(self, to: Self) -> bool {
        use Domain::*;
        match (self, to) {
            (Date | Time | Timestamp, Timestamp)
            | (Timestamp, Date)
            | (ShortInterval { .. }, ShortInterval { .. })
            | (LongInterval { .. }, LongInterval { .. })
            | (
                Timestamp,
                Integer {
                    bits: 64,
                    signed: true,
                },
            )
            | (Integer { .. } | Decimal { .. } | Float | Timestamp, Float) => true,
            (
                Integer {
                    bits: a,
                    signed: signed_a,
                },
                Integer {
                    bits: b,
                    signed: signed_b,
                },
            ) => (signed_b || !signed_a) && a - u32::from(signed_a) <= b - u32::from(signed_b),
            (
                Integer { bits, signed },
                Decimal {
                    integral_digits, ..
                },
            ) => {
                let (negative, positive) = magnitudes(bits, signed);
                negative.max(positive) < 10_u128.pow(integral_digits)
            }
            (
                Decimal {
                    integral_digits, ..
                },
                Integer { bits, signed },
            ) => {
                // Rounding can attain either endpoint, including the next power of ten.
                let (negative, positive) = magnitudes(bits, signed);
                negative.min(positive) >= 10_u128.pow(integral_digits)
            }
            (
                Decimal {
                    integral_digits: a,
                    scale: s,
                },
                Decimal {
                    integral_digits: b,
                    scale: t,
                },
            ) => a <= b && s <= t,
            _ => false,
        }
    }

    fn has_saturating_cast(self, to: Self) -> bool {
        use Domain::*;
        let narrow = |ty| {
            matches!(
                ty,
                Integer {
                    bits: 8 | 16 | 32 | 64,
                    ..
                }
            )
        };
        let single_interval = |ty| {
            matches!(
                ty,
                ShortInterval { single_unit: true } | LongInterval { single_unit: true }
            )
        };
        if single_interval(self) || self == Timestamp {
            return narrow(to) || matches!(to, Decimal { .. });
        }
        if single_interval(to) {
            return narrow(self) || matches!(self, Decimal { .. });
        }
        match to {
            Timestamp => narrow(self) || matches!(self, Float | Decimal { .. }),
            Integer { .. } => matches!(self, Integer { .. } | Decimal { .. } | Float),
            Decimal { .. } => narrow(self) || matches!(self, Decimal { .. } | Float),
            _ => false,
        }
    }
}

fn attribute(node: &Node, name: &str) -> Result<u32, Error> {
    node.data(name)
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| Error::new(format!("invalid {name} in waterline cast")))
}

/// Absolute values of the negative and positive endpoints, including unsigned 128-bit.
fn magnitudes(bits: u32, signed: bool) -> (u128, u128) {
    let mask = u128::MAX >> (128 - bits);
    if signed {
        ((mask >> 1) + 1, mask >> 1)
    } else {
        (0, mask)
    }
}

pub fn cast_waterline(
    edit: &CircuitEditor,
    from: TypeId,
    to: TypeId,
    bound: TypeId,
    safe: bool,
) -> Result<CastWaterline, Error> {
    let target = Domain::read(edit, to)?;
    let source = Domain::read(edit, from)?;
    // TIME wraps at midnight. Numeric-to-BOOLEAN maps both signs to true.
    if target == Domain::Other || matches!(target, Domain::Time | Domain::Bool) && source != target
    {
        return Ok(CastWaterline::None);
    }
    let total = edit.same_type_ignoring_nullability(from, to)? || source.fits_in(target);
    // A failing SAFE_CAST can insert NULL between two non-null results.
    if safe && !total {
        return Ok(CastWaterline::None);
    }
    if !edit.nullable(bound)? && edit.same_type_ignoring_nullability(bound, to)? {
        return Ok(CastWaterline::Identity);
    }
    Ok(if total {
        CastWaterline::Ordinary
    } else if Domain::read(edit, bound)?.has_saturating_cast(target) {
        CastWaterline::Saturating
    } else {
        CastWaterline::None
    })
}

#[cfg(test)]
mod tests {
    use super::Domain::{Decimal, Integer, LongInterval, Timestamp};

    #[test]
    fn integer_and_decimal_endpoints_include_rounding() {
        let signed = Integer {
            bits: 8,
            signed: true,
        };
        let unsigned = Integer {
            bits: 8,
            signed: false,
        };
        let two_digits = Decimal {
            integral_digits: 2,
            scale: 3,
        };
        let three_digits = Decimal {
            integral_digits: 3,
            scale: 0,
        };
        assert!(two_digits.fits_in(signed));
        assert!(!two_digits.fits_in(unsigned));
        assert!(!signed.fits_in(two_digits));
        assert!(unsigned.fits_in(three_digits));
        assert!(!three_digits.fits_in(signed));
        assert!(!signed.fits_in(unsigned));
        assert!(!unsigned.fits_in(signed));
        assert!(!two_digits.fits_in(three_digits));
    }

    #[test]
    fn saturating_temporal_casts_require_a_narrow_integer_or_decimal() {
        let wide = Integer {
            bits: 128,
            signed: false,
        };
        let narrow = Integer {
            bits: 64,
            signed: true,
        };
        for ty in [Timestamp, LongInterval { single_unit: true }] {
            assert!(ty.has_saturating_cast(narrow));
            assert!(!ty.has_saturating_cast(wide));
            assert!(narrow.has_saturating_cast(ty));
            assert!(!wide.has_saturating_cast(ty));
        }
        assert!(!LongInterval { single_unit: false }.has_saturating_cast(narrow));
    }
}
