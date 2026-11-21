//! SQL type information retained by both disposable inference strategies.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Kind {
    Boolean,
    Integer { bits: u8 },
    Real,
    Double,
    Decimal { precision: u8, scale: u8 },
    Character { fixed: bool, length: Option<u32> },
    Binary { fixed: bool, length: Option<u32> },
    Date,
    Timestamp { precision: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlType {
    pub kind: Kind,
    pub nullable: bool,
}

impl SqlType {
    /// Reject malformed descriptors supplied by callers other than the parser.
    pub fn is_valid(self) -> bool {
        match self.kind {
            Kind::Integer { bits } => matches!(bits, 8 | 16 | 32 | 64),
            Kind::Decimal { precision, scale } => {
                (1..=38).contains(&precision) && scale <= precision
            }
            Kind::Character { length, .. } | Kind::Binary { length, .. } => {
                length.is_none_or(|n| n > 0 && n <= i32::MAX as u32)
            }
            Kind::Timestamp { precision } => precision == 6,
            _ => true,
        }
    }
    /// Parse only the declared scalar probe grammar, never an arbitrary SQL AST.
    pub fn parse(text: &str, nullable: bool) -> Result<Self, String> {
        let integer = |bits| Kind::Integer { bits };
        let kind = match text {
            "BOOLEAN" => Kind::Boolean,
            "TINYINT" => integer(8),
            "SMALLINT" => integer(16),
            "INTEGER" => integer(32),
            "BIGINT" => integer(64),
            "REAL" => Kind::Real,
            "DOUBLE" => Kind::Double,
            "DATE" => Kind::Date,
            "TIMESTAMP" => Kind::Timestamp { precision: 6 },
            "VARCHAR" => Kind::Character {
                fixed: false,
                length: None,
            },
            _ => {
                let (name, parameters) = text
                    .split_once('(')
                    .ok_or_else(|| format!("unsupported probe type: {text}"))?;
                let parameters = parameters
                    .strip_suffix(')')
                    .ok_or_else(|| format!("invalid type parameters: {text}"))?;
                match name {
                    "DECIMAL" => {
                        let (precision, scale) = parameters
                            .split_once(',')
                            .ok_or_else(|| format!("decimal needs precision and scale: {text}"))?;
                        let precision: u8 = precision
                            .parse()
                            .map_err(|_| format!("invalid precision: {text}"))?;
                        let scale: u8 = scale
                            .parse()
                            .map_err(|_| format!("invalid scale: {text}"))?;
                        if precision == 0 || precision > 38 || scale > precision {
                            return Err(format!("decimal outside prototype limits: {text}"));
                        }
                        Kind::Decimal { precision, scale }
                    }
                    "CHAR" | "VARCHAR" => {
                        let length: u32 = parameters
                            .parse()
                            .map_err(|_| format!("invalid length: {text}"))?;
                        if length == 0 || length > i32::MAX as u32 {
                            return Err(format!("character length outside limits: {text}"));
                        }
                        Kind::Character {
                            fixed: name == "CHAR",
                            length: Some(length),
                        }
                    }
                    _ => return Err(format!("unsupported probe type: {text}")),
                }
            }
        };
        Ok(Self { kind, nullable })
    }

    pub fn with_kind(self, kind: Kind) -> Self {
        Self { kind, ..self }
    }

    pub fn numeric(self) -> bool {
        matches!(
            self.kind,
            Kind::Integer { .. } | Kind::Decimal { .. } | Kind::Real | Kind::Double
        )
    }

    pub fn character(self) -> bool {
        matches!(self.kind, Kind::Character { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum DerivedType {
    Scalar(SqlType),
    Array {
        component: Box<DerivedType>,
        nullable: bool,
    },
}

impl From<SqlType> for DerivedType {
    fn from(value: SqlType) -> Self {
        Self::Scalar(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Bound {
        result: DerivedType,
        operands: Vec<SqlType>,
    },
    Rejected {
        reason: &'static str,
    },
    Unsupported {
        reason: &'static str,
    },
}

impl Outcome {
    pub fn bound(result: impl Into<DerivedType>, operands: Vec<SqlType>) -> Self {
        Self::Bound {
            result: result.into(),
            operands,
        }
    }
    pub fn rejected() -> Self {
        Self::Rejected {
            reason: "incompatible operand types",
        }
    }
}
