//! Bounded payload checks. SQL type/value compatibility belongs to the S1 adapter.

use crate::{
    ValidationError,
    literals::{Literal, LiteralValue, valid_integer},
};

#[derive(Clone, Copy, Debug)]
pub struct LiteralLimits {
    pub nodes: usize,
    pub depth: usize,
    pub payload_bytes: usize,
}

impl Default for LiteralLimits {
    fn default() -> Self {
        Self {
            nodes: 100_000,
            depth: 256,
            payload_bytes: 16 * 1024 * 1024,
        }
    }
}

fn invalid(message: &'static str) -> ValidationError {
    ValidationError {
        path: "literal.value".into(),
        message,
    }
}

impl Literal {
    /// Check every nested payload without recursion. This does not resolve SQL
    /// type/source references or establish that a payload fits its declared type.
    /// A caller must also bound input bytes before deserialization.
    pub fn validate_payload(&self, limits: LiteralLimits) -> Result<(), ValidationError> {
        let mut pending = vec![(self, 1)];
        let mut remaining_nodes = limits.nodes;
        let mut remaining_bytes = limits.payload_bytes;
        while let Some((literal, depth)) = pending.pop() {
            remaining_nodes = remaining_nodes
                .checked_sub(1)
                .ok_or_else(|| invalid("literal node limit exceeded"))?;
            if depth > limits.depth {
                return Err(invalid("literal depth limit exceeded"));
            }
            let mut bytes = |count: usize| -> Result<(), ValidationError> {
                remaining_bytes = remaining_bytes
                    .checked_sub(count)
                    .ok_or_else(|| invalid("literal payload byte limit exceeded"))?;
                Ok(())
            };
            let mut children = Vec::new();
            match &literal.value {
                LiteralValue::Null {}
                | LiteralValue::Boolean { .. }
                | LiteralValue::Float32 { .. }
                | LiteralValue::Float64 { .. }
                | LiteralValue::Date { .. }
                | LiteralValue::Uuid { .. } => {}
                LiteralValue::Integer { value } => {
                    bytes(value.len())?;
                    if !valid_integer(value) {
                        return Err(invalid("invalid integer coefficient"));
                    }
                }
                LiteralValue::Decimal { value } | LiteralValue::Interval { value, .. } => {
                    bytes(value.coefficient.len())?;
                    if !value.has_valid_coefficient() {
                        return Err(invalid("invalid decimal coefficient"));
                    }
                }
                LiteralValue::GeometryPoint {
                    dimension,
                    measures,
                    ordinates,
                    ..
                } => {
                    if *measures > 1
                        || dimension
                            .checked_sub(*measures)
                            .is_none_or(|spatial| !(2..=3).contains(&spatial))
                        || (!ordinates.is_empty() && ordinates.len() != usize::from(*dimension))
                    {
                        return Err(invalid("invalid point coordinate dimensions"));
                    }
                    bytes(ordinates.len() * size_of::<u64>())?;
                }
                LiteralValue::Character { value } => bytes(value.len())?,
                LiteralValue::Binary { value } => bytes(value.len())?,
                LiteralValue::Time {
                    nanoseconds_since_midnight,
                }
                | LiteralValue::Timestamp {
                    nanoseconds_since_midnight,
                    ..
                } => {
                    if *nanoseconds_since_midnight >= 86_400_000_000_000 {
                        return Err(invalid("time is outside the day"));
                    }
                }
                LiteralValue::LocalTimestampWithZone {
                    nanoseconds_since_midnight,
                    zone,
                    ..
                } => {
                    bytes(zone.len())?;
                    if *nanoseconds_since_midnight >= 86_400_000_000_000 || zone.is_empty() {
                        return Err(invalid("invalid local zoned timestamp fields"));
                    }
                }
                LiteralValue::TimestampWithZone {
                    nanoseconds, zone, ..
                } => {
                    bytes(zone.len())?;
                    if *nanoseconds >= 1_000_000_000 || zone.is_empty() {
                        return Err(invalid("invalid zoned timestamp fields"));
                    }
                }
                LiteralValue::Symbol { domain, name } => {
                    bytes(domain.len())?;
                    bytes(name.len())?;
                    if !supported_symbol(domain, name) {
                        return Err(invalid("unsupported symbol domain or member"));
                    }
                }
                LiteralValue::Array { elements } | LiteralValue::Row { fields: elements } => {
                    check_children(elements.len(), pending.len(), remaining_nodes)?;
                    children.extend(elements.iter());
                }
                LiteralValue::Map { entries } => {
                    let count = entries
                        .len()
                        .checked_mul(2)
                        .ok_or_else(|| invalid("literal node limit exceeded"))?;
                    check_children(count, pending.len(), remaining_nodes)?;
                    for entry in entries {
                        children.extend([&entry.key, &entry.value]);
                    }
                }
                LiteralValue::Multiset { entries } => {
                    check_children(entries.len(), pending.len(), remaining_nodes)?;
                    for entry in entries {
                        bytes(entry.multiplicity.len())?;
                        if !valid_integer(&entry.multiplicity)
                            || entry.multiplicity.starts_with('-')
                        {
                            return Err(invalid("multiset multiplicity must be nonnegative"));
                        }
                        children.push(&entry.value);
                    }
                }
            }
            pending.extend(children.into_iter().map(|child| (child, depth + 1)));
        }
        Ok(())
    }
}

fn check_children(
    children: usize,
    pending: usize,
    remaining: usize,
) -> Result<(), ValidationError> {
    if children
        .checked_add(pending)
        .is_none_or(|count| count > remaining)
    {
        return Err(invalid("literal node limit exceeded"));
    }
    Ok(())
}

// Keep the supported domains aligned with the Java literal adapter. A nonempty
// string is not sufficient: arbitrary enum names cannot be reconstructed there.
fn supported_symbol(domain: &str, name: &str) -> bool {
    match domain {
        "trim_mode" => matches!(name, "BOTH" | "LEADING" | "TRAILING"),
        "time_unit_range" => matches!(
            name,
            "YEAR"
                | "YEAR_TO_MONTH"
                | "MONTH"
                | "DAY"
                | "DAY_TO_HOUR"
                | "DAY_TO_MINUTE"
                | "DAY_TO_SECOND"
                | "HOUR"
                | "HOUR_TO_MINUTE"
                | "HOUR_TO_SECOND"
                | "MINUTE"
                | "MINUTE_TO_SECOND"
                | "SECOND"
                | "ISOYEAR"
                | "QUARTER"
                | "WEEK"
                | "MILLISECOND"
                | "MICROSECOND"
                | "NANOSECOND"
                | "DOW"
                | "ISODOW"
                | "DOY"
                | "EPOCH"
                | "DECADE"
                | "CENTURY"
                | "MILLENNIUM"
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TypeId, literals::ExactDecimal, source::Origin};

    fn node(value: LiteralValue) -> Literal {
        Literal {
            ty: TypeId(0),
            origin: Origin::Absent {},
            value,
        }
    }

    #[test]
    fn invalid_payloads_cannot_hide_in_nested_values() {
        for value in [
            LiteralValue::Decimal {
                value: ExactDecimal {
                    coefficient: "1e3".into(),
                    scale: 0,
                },
            },
            LiteralValue::Time {
                nanoseconds_since_midnight: 86_400_000_000_000,
            },
            LiteralValue::TimestampWithZone {
                epoch_seconds: 0,
                nanoseconds: 1_000_000_000,
                zone: "UTC".into(),
            },
        ] {
            let value = node(LiteralValue::Array {
                elements: vec![node(value)],
            });
            assert!(value.validate_payload(LiteralLimits::default()).is_err());
        }
    }

    #[test]
    fn node_depth_and_byte_limits_are_enforced_independently() {
        let value = node(LiteralValue::Array {
            elements: vec![node(LiteralValue::Character {
                value: "abc".into(),
            })],
        });
        assert!(
            value
                .validate_payload(LiteralLimits {
                    nodes: 2,
                    depth: 2,
                    payload_bytes: 3
                })
                .is_ok()
        );
        for limits in [
            LiteralLimits {
                nodes: 1,
                depth: 2,
                payload_bytes: 3,
            },
            LiteralLimits {
                nodes: 2,
                depth: 1,
                payload_bytes: 3,
            },
            LiteralLimits {
                nodes: 2,
                depth: 2,
                payload_bytes: 2,
            },
        ] {
            assert!(value.validate_payload(limits).is_err());
        }
    }
}
