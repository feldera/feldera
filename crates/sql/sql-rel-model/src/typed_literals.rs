//! Structural type checks for literal values. These checks do not perform SQL
//! coercions, character encoding conversion, or planner-specific normalization.

use crate::{
    LiteralLimits, TypeId, TypeTable, ValidationError, ValidationLimits,
    literals::{ExactDecimal, IntervalStorageUnit, Literal, LiteralValue},
    types::{Cardinality, FloatFlavor, IntegerWidth, IntervalUnit, Precision, TimeZone, TypeKind},
};

fn error(path: &str, message: &'static str) -> ValidationError {
    ValidationError {
        path: path.into(),
        message,
    }
}

/// An immutably borrowed arena whose references and acyclicity have been checked.
/// Reuse this context across literals instead of traversing the type graph again.
pub struct ValidatedTypes<'a> {
    table: &'a TypeTable,
}

impl ValidatedTypes<'_> {
    /// Check one independently bounded literal against the validated arena.
    pub fn validate_literal(
        &self,
        literal: &Literal,
        limits: LiteralLimits,
    ) -> Result<(), ValidationError> {
        self.table.validate_literal_in_valid_arena(literal, limits)
    }
}

impl TypeTable {
    /// Validate the arena, payload limits, source/type references, and supported
    /// type/value constraints. Nested values must retain the exact type IDs in
    /// their container's declaration; structurally equal types are not interned.
    ///
    /// Unsupported non-null types fail closed. This is a draft contract check,
    /// not evidence of Calcite import compatibility. Bound wire bytes before
    /// deserialization as well as the graph and payload sizes here.
    pub fn validate_literal(
        &self,
        literal: &Literal,
        type_limits: ValidationLimits,
        literal_limits: LiteralLimits,
    ) -> Result<(), ValidationError> {
        self.validated(type_limits)?
            .validate_literal(literal, literal_limits)
    }

    /// Establish graph invariants once; the borrow prevents mutation while the
    /// returned context is used. This does not establish Java lowering support.
    pub fn validated(
        &self,
        limits: ValidationLimits,
    ) -> Result<ValidatedTypes<'_>, ValidationError> {
        self.validate(limits)?;
        Ok(ValidatedTypes { table: self })
    }

    fn validate_literal_in_valid_arena(
        &self,
        literal: &Literal,
        literal_limits: LiteralLimits,
    ) -> Result<(), ValidationError> {
        literal.validate_payload(literal_limits)?;
        let mut pending = vec![(literal, String::from("literal"))];
        while let Some((literal, path)) = pending.pop() {
            self.reference(literal.ty, &format!("{path}.ty"))?;
            self.origin(&literal.origin, &format!("{path}.origin"))?;
            let declared = &self.types[literal.ty.0 as usize];
            if matches!(literal.value, LiteralValue::Null {}) {
                if !declared.nullable && !matches!(declared.kind, TypeKind::Null {}) {
                    return Err(error(&path, "NULL requires a nullable declared type"));
                }
                continue;
            }
            // The arena verifier has already rejected cycles and bounded depth.
            // Named type identity stays on the literal; only its representation
            // determines which payload forms it can carry.
            let mut kind = &declared.kind;
            while let TypeKind::Named { representation, .. } = kind {
                kind = &self.types[representation.0 as usize].kind;
            }
            self.check_literal_value(literal, kind, &path, &mut pending)?;
        }
        Ok(())
    }

    fn check_literal_value<'a>(
        &self,
        literal: &'a Literal,
        kind: &TypeKind,
        path: &str,
        pending: &mut Vec<(&'a Literal, String)>,
    ) -> Result<(), ValidationError> {
        use LiteralValue as V;
        use TypeKind as T;
        match (kind, &literal.value) {
            (T::Boolean {}, V::Boolean { .. })
            | (T::Date {}, V::Date { .. })
            | (T::Uuid {}, V::Uuid { .. })
            | (T::Geometry { .. }, V::GeometryPoint { .. })
            | (T::Symbol {}, V::Symbol { .. })
            | (
                T::Float {
                    flavor: FloatFlavor::Real,
                },
                V::Float32 { .. },
            )
            | (
                T::Float {
                    flavor: FloatFlavor::Float | FloatFlavor::Double,
                },
                V::Float64 { .. },
            ) => {}
            (T::Integer { width, signed }, V::Integer { value }) => {
                if !integer_fits(value, *width, *signed) {
                    return Err(error(path, "integer exceeds its declared range"));
                }
            }
            (
                T::Decimal {
                    precision, scale, ..
                },
                V::Decimal { value },
            ) => {
                if !decimal_fits(value, *precision, *scale) {
                    return Err(error(
                        path,
                        "decimal cannot fit its declared precision and scale exactly",
                    ));
                }
            }
            (T::Character { length, .. }, V::Character { value }) => {
                // The Java adapter measures character precision in UTF-16 code units.
                check_length(value.encode_utf16().count(), *length, path)?;
            }
            (T::Binary { length, .. }, V::Binary { value }) => {
                check_length(value.len(), *length, path)?;
            }
            (
                T::Time {
                    precision,
                    zone: TimeZone::None,
                },
                V::Time {
                    nanoseconds_since_midnight,
                },
            )
            | (
                T::Timestamp {
                    precision,
                    zone: TimeZone::None,
                },
                V::Timestamp {
                    nanoseconds_since_midnight,
                    ..
                },
            ) => {
                check_fraction(*nanoseconds_since_midnight, *precision, path)?;
            }
            (
                T::Timestamp {
                    precision,
                    zone: TimeZone::WithTimeZone,
                },
                V::TimestampWithZone { nanoseconds, .. },
            ) => {
                check_fraction(u64::from(*nanoseconds), *precision, path)?;
            }
            (
                T::Timestamp {
                    precision,
                    zone: TimeZone::WithTimeZone,
                },
                V::LocalTimestampWithZone {
                    nanoseconds_since_midnight,
                    ..
                },
            ) => {
                check_fraction(*nanoseconds_since_midnight, *precision, path)?;
            }
            (T::Interval { start, .. }, V::Interval { unit, .. }) => {
                let expected = if matches!(
                    start,
                    IntervalUnit::Year | IntervalUnit::Quarter | IntervalUnit::Month
                ) {
                    IntervalStorageUnit::Months
                } else {
                    IntervalStorageUnit::Milliseconds
                };
                if *unit != expected {
                    return Err(error(path, "interval payload uses the wrong storage unit"));
                }
            }
            (
                T::Array {
                    element,
                    cardinality,
                },
                V::Array { elements },
            ) => {
                check_cardinality(elements.len(), *cardinality, path)?;
                for (index, child) in elements.iter().enumerate() {
                    push_child(
                        pending,
                        child,
                        *element,
                        format!("{path}.elements[{index}]"),
                    )?;
                }
            }
            (T::Row { fields, .. }, V::Row { fields: values }) => {
                if fields.len() != values.len() {
                    return Err(error(
                        path,
                        "row field count differs from its declared type",
                    ));
                }
                for (index, (field, child)) in fields.iter().zip(values).enumerate() {
                    push_child(pending, child, field.ty, format!("{path}.fields[{index}]"))?;
                }
            }
            (T::Map { key, value }, V::Map { entries }) => {
                for (index, entry) in entries.iter().enumerate() {
                    push_child(
                        pending,
                        &entry.key,
                        *key,
                        format!("{path}.entries[{index}].key"),
                    )?;
                    push_child(
                        pending,
                        &entry.value,
                        *value,
                        format!("{path}.entries[{index}].value"),
                    )?;
                }
            }
            (
                T::Multiset {
                    element,
                    cardinality,
                },
                V::Multiset { entries },
            ) => {
                let mut total = 0u64;
                for (index, entry) in entries.iter().enumerate() {
                    if let Cardinality::Bounded(bound) = cardinality {
                        // Reject an overlarge count without constructing a bigint.
                        let count = entry.multiplicity.parse::<u64>().map_err(|_| {
                            error(path, "multiset exceeds its declared cardinality")
                        })?;
                        total = total
                            .checked_add(count)
                            .filter(|total| total <= bound)
                            .ok_or_else(|| {
                                error(path, "multiset exceeds its declared cardinality")
                            })?;
                    }
                    push_child(
                        pending,
                        &entry.value,
                        *element,
                        format!("{path}.entries[{index}].value"),
                    )?;
                }
            }
            _ => {
                return Err(error(
                    path,
                    "unsupported literal payload for its declared SQL type",
                ));
            }
        }
        Ok(())
    }
}

fn push_child<'a>(
    pending: &mut Vec<(&'a Literal, String)>,
    child: &'a Literal,
    expected: TypeId,
    path: String,
) -> Result<(), ValidationError> {
    if child.ty != expected {
        return Err(error(
            &format!("{path}.ty"),
            "nested literal type ID differs from its declaration",
        ));
    }
    pending.push((child, path));
    Ok(())
}

pub(crate) fn integer_fits(value: &str, width: IntegerWidth, signed: bool) -> bool {
    let bits = match width {
        IntegerWidth::I8 => 8,
        IntegerWidth::I16 => 16,
        IntegerWidth::I32 => 32,
        IntegerWidth::I64 => 64,
    };
    // Payload syntax is already checked; i128 covers every declared integer.
    let Ok(value) = value.parse::<i128>() else {
        return false;
    };
    if signed {
        (-(1i128 << (bits - 1))..(1i128 << (bits - 1))).contains(&value)
    } else {
        (0..(1i128 << bits)).contains(&value)
    }
}

pub(crate) fn decimal_fits(value: &ExactDecimal, precision: u32, scale: u32) -> bool {
    let digits = value
        .coefficient
        .trim_start_matches(['+', '-'])
        .trim_start_matches('0');
    if digits.is_empty() {
        return true;
    }
    // Count the coefficient digits after an exact rescale, without allocating
    // powers of ten or iterating over an untrusted scale.
    let adjustment = i64::from(scale) - i64::from(value.scale);
    if adjustment >= 0 {
        digits.len() as u64 + adjustment as u64 <= u64::from(precision)
    } else {
        let removed = (-adjustment) as u64;
        removed
            <= digits
                .bytes()
                .rev()
                .take_while(|byte| *byte == b'0')
                .count() as u64
            && digits.len() as u64 - removed <= u64::from(precision)
    }
}

fn check_length(length: usize, precision: Precision, path: &str) -> Result<(), ValidationError> {
    if !precision.contains_length(length) {
        return Err(error(path, "literal exceeds its declared length"));
    }
    Ok(())
}

fn check_cardinality(
    length: usize,
    cardinality: Cardinality,
    path: &str,
) -> Result<(), ValidationError> {
    if let Cardinality::Bounded(maximum) = cardinality
        && length as u64 > maximum
    {
        return Err(error(path, "array exceeds its declared cardinality"));
    }
    Ok(())
}

fn check_fraction(
    nanoseconds: u64,
    precision: Precision,
    path: &str,
) -> Result<(), ValidationError> {
    if !precision.contains_fraction(nanoseconds) {
        return Err(error(
            path,
            "temporal payload exceeds its declared fractional precision",
        ));
    }
    Ok(())
}
