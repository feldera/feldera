//! Render datetime literals once for VALUES, projected expressions and the profiler plan.

use super::{clock_text, range, Error, Ids, Value};
use feldera_sql_rel_model::{
    literals::{date_to_text, LiteralValue},
    types::{Precision, TimeZone, TypeKind},
};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum TemporalKind {
    Date,
    Time(u32),
    Timestamp(u32),
}

impl TemporalKind {
    pub(crate) fn sql_name(self) -> &'static str {
        match self {
            Self::Date => "DATE",
            Self::Time(_) => "TIME",
            Self::Timestamp(_) => "TIMESTAMP",
        }
    }

    pub(crate) fn lower_type(self, ids: &mut Ids) -> Value {
        let class = match self {
            Self::Date => "DBSPTypeDate",
            Self::Time(_) => "DBSPTypeTime",
            Self::Timestamp(_) => "DBSPTypeTimestamp",
        };
        let mut ty = ids.node(class, &range(0, 0, 0, 0));
        ty["code"] = self.sql_name().into();
        ty["mayBeNull"] = false.into();
        ty
    }
}

pub(crate) struct TemporalLiteral {
    pub(crate) kind: TemporalKind,
    pub(crate) digest: String,
    pub(crate) sql: String,
    value: Value,
}

impl TemporalLiteral {
    pub(crate) fn resolve(ty: &TypeKind, value: &LiteralValue) -> Result<Self, Error> {
        let (kind, digest, text, value) = match (ty, value) {
            (TypeKind::Date {}, LiteralValue::Date { epoch_days }) => {
                let date = date_to_text(*epoch_days)
                    .ok_or_else(|| Error("DATE literal is outside the SQL year range".into()))?;
                (
                    TemporalKind::Date,
                    date.clone(),
                    date,
                    Value::from(*epoch_days),
                )
            }
            (
                TypeKind::Time {
                    precision: Precision::Specified(p),
                    zone: TimeZone::None,
                },
                LiteralValue::Time {
                    nanoseconds_since_midnight,
                },
            ) => {
                let (time, digest) = clock_text(*nanoseconds_since_midnight, *p)?;
                (
                    TemporalKind::Time(*p),
                    digest.clone(),
                    time,
                    Value::from(digest),
                )
            }
            (
                TypeKind::Timestamp {
                    precision: Precision::Specified(p),
                    zone: TimeZone::None,
                },
                LiteralValue::Timestamp {
                    epoch_days,
                    nanoseconds_since_midnight,
                },
            ) if *p <= 6 => {
                let date = date_to_text(*epoch_days)
                    .ok_or_else(|| Error("timestamp outside the SQL date range".into()))?;
                let (time, digest) = clock_text(*nanoseconds_since_midnight, *p)?;
                let micros = i64::from(*epoch_days) * 86_400_000_000
                    + (nanoseconds_since_midnight / 1000) as i64;
                (
                    TemporalKind::Timestamp(*p),
                    format!("{date} {digest}"),
                    format!("{date} {time}"),
                    Value::from(micros.to_string()),
                )
            }
            _ => {
                return Err(Error(
                    "expected a local datetime literal with its declared type".into(),
                ))
            }
        };
        Ok(Self {
            kind,
            digest,
            sql: format!("{} '{text}'", kind.sql_name()),
            value,
        })
    }

    pub(crate) fn expression(&self, ids: &mut Ids) -> Value {
        let class = match self.kind {
            TemporalKind::Date => "DBSPDateLiteral",
            TemporalKind::Time(_) => "DBSPTimeLiteral",
            TemporalKind::Timestamp(_) => "DBSPTimestampLiteral",
        };
        let mut literal = ids.node(class, &range(0, 0, 0, 0));
        literal["value"] = self.value.clone();
        literal
    }

    pub(crate) fn typed_digest(&self) -> String {
        match self.kind {
            TemporalKind::Time(p) | TemporalKind::Timestamp(p) if p > 0 => {
                format!("{}:{}({p})", self.digest, self.kind.sql_name())
            }
            _ => self.digest.clone(),
        }
    }
}
