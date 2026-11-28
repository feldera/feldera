//! The Feldera datetime text functions stay as calls in the relational plan.

use super::OperatorObject;
use crate::{
    source::Origin,
    types::{CharacterMetadata, Precision, SqlType, TimeZone, TypeKind},
};

const CLASS_PREFIX: &str =
    "org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions$";

#[derive(Clone, Copy)]
enum TemporalKind {
    Date,
    Time,
    Timestamp,
}

impl TemporalKind {
    fn matches(self, ty: &TypeKind) -> bool {
        matches!(
            (self, ty),
            (Self::Date, TypeKind::Date {})
                | (
                    Self::Time,
                    TypeKind::Time {
                        zone: TimeZone::None,
                        ..
                    }
                )
                | (
                    Self::Timestamp,
                    TypeKind::Timestamp {
                        zone: TimeZone::None,
                        ..
                    }
                )
        )
    }

    fn result(self) -> TypeKind {
        match self {
            Self::Date => TypeKind::Date {},
            Self::Time => TypeKind::Time {
                precision: Precision::Specified(0),
                zone: TimeZone::None,
            },
            Self::Timestamp => TypeKind::Timestamp {
                precision: Precision::Specified(6),
                zone: TimeZone::None,
            },
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Parse(TemporalKind),
    Format(TemporalKind),
}

/// One SQL signature and its matching identity in the S1 operator registry.
pub struct TemporalTextFunction {
    pub sql_name: &'static str,
    registry_suffix: &'static str,
    operation: Operation,
}

use Operation::{Format, Parse};
use TemporalKind::{Date, Time, Timestamp};

const FUNCTIONS: &[TemporalTextFunction] = &[
    TemporalTextFunction::new(
        "FORMAT_DATE",
        "FormatDateFunction#format_date",
        Format(Date),
    ),
    TemporalTextFunction::new(
        "FORMAT_TIME",
        "FormatTimeFunction#format_time",
        Format(Time),
    ),
    TemporalTextFunction::new(
        "FORMAT_TIMESTAMP",
        "FormatTimestampFunction#format_timestamp",
        Format(Timestamp),
    ),
    TemporalTextFunction::new("PARSE_DATE", "ParseDateFunction#parse_date", Parse(Date)),
    TemporalTextFunction::new("PARSE_TIME", "ParseTimeFunction#parse_time", Parse(Time)),
    TemporalTextFunction::new(
        "PARSE_TIMESTAMP",
        "ParseTimestampFunction#parse_timestamp",
        Parse(Timestamp),
    ),
];

impl TemporalTextFunction {
    const fn new(
        sql_name: &'static str,
        registry_suffix: &'static str,
        operation: Operation,
    ) -> Self {
        Self {
            sql_name,
            registry_suffix,
            operation,
        }
    }

    pub fn named(name: &str) -> Option<&'static Self> {
        FUNCTIONS
            .iter()
            .find(|f| f.sql_name.eq_ignore_ascii_case(name))
    }

    pub fn from_key(key: &str) -> Option<&'static Self> {
        let suffix = key.strip_prefix(CLASS_PREFIX)?;
        FUNCTIONS.iter().find(|f| f.registry_suffix == suffix)
    }

    pub fn operator(&self) -> OperatorObject {
        OperatorObject::Builtin {
            registry: "feldera-s1-builtins".into(),
            registry_version: "1".into(),
            key: format!("{CLASS_PREFIX}{}", self.registry_suffix),
        }
    }

    /// The validator casts character values supplied to FORMAT to these types.
    pub fn format_input(&self) -> Option<(TypeKind, &'static str)> {
        Some(match self.operation {
            Format(Date) => (Date.result(), "DATE"),
            Format(Time) => (
                TypeKind::Time {
                    precision: Precision::Specified(9),
                    zone: TimeZone::None,
                },
                "TIME(9)",
            ),
            Format(Timestamp) => (Timestamp.result(), "TIMESTAMP(6)"),
            Parse(_) => return None,
        })
    }

    pub fn parses_text(&self) -> bool {
        matches!(self.operation, Parse(_))
    }

    pub fn result(&self, pattern: &SqlType, value: &SqlType) -> Option<SqlType> {
        let text = |kind: &TypeKind| matches!(kind, TypeKind::Character { .. } | TypeKind::Null {});
        if !text(&pattern.kind) {
            return None;
        }
        let (kind, nullable) = match self.operation {
            Parse(temporal) if text(&value.kind) => (temporal.result(), true),
            Format(temporal)
                if temporal.matches(&value.kind) || matches!(value.kind, TypeKind::Null {}) =>
            {
                (
                    TypeKind::Character {
                        fixed: false,
                        length: Precision::Unspecified,
                        metadata: CharacterMetadata::implicit(),
                    },
                    pattern.nullable || value.nullable,
                )
            }
            _ => return None,
        };
        Some(SqlType {
            kind,
            nullable,
            origin: Origin::Absent {},
        })
    }
}
