use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use feldera_fxp::DynamicDecimal;
use serde::Deserialize;
use serde_json::{Value as Json, json};

/// A projection of the fold journal, not a replacement for the compiler's IR.
#[derive(Deserialize)]
pub struct Expression {
    pub kind: String,
    #[serde(rename = "type")]
    pub ty: SqlType,
    pub operator: Option<String>,
    pub operator_class: Option<String>,
    pub builtin: Option<String>,
    pub deterministic: Option<bool>,
    #[serde(default)]
    pub dynamic: bool,
    #[serde(default)]
    pub arguments: Vec<Expression>,
    pub value: Option<String>,
}

#[derive(Deserialize, PartialEq, Eq)]
pub struct SqlType {
    pub name: String,
    pub precision: i32,
    pub scale: i32,
    pub component: Option<Box<SqlType>>,
    pub key: Option<Box<SqlType>>,
    pub value: Option<Box<SqlType>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Decimal(DynamicDecimal),
    String(String),
    Binary(Vec<u8>),
    Uuid(u128),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Months(i32),
    Micros(i64),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Point(f64, f64),
}

impl Value {
    /// Conservative retained size, including spare collection capacity. The
    /// evaluator charges each intermediate value, even when later moved/reused.
    pub fn retained_bytes(&self) -> Result<usize> {
        let payload = match self {
            Self::String(s) => s.capacity(),
            Self::Binary(bytes) => bytes.capacity(),
            Self::Array(values) => {
                let spare = values
                    .capacity()
                    .saturating_sub(values.len())
                    .checked_mul(std::mem::size_of::<Self>())
                    .ok_or(Refusal::Budget)?;
                values.iter().try_fold(spare, |size, value| {
                    size.checked_add(value.retained_bytes()?)
                        .ok_or(Refusal::Budget)
                })?
            }
            Self::Map(entries) => {
                let spare = entries
                    .capacity()
                    .saturating_sub(entries.len())
                    .checked_mul(std::mem::size_of::<(Self, Self)>())
                    .ok_or(Refusal::Budget)?;
                entries.iter().try_fold(spare, |size, (key, value)| {
                    let key_bytes = key.retained_bytes()?;
                    let value_bytes = value.retained_bytes()?;
                    size.checked_add(key_bytes)
                        .and_then(|n| n.checked_add(value_bytes))
                        .ok_or(Refusal::Budget)
                })?
            }
            _ => 0,
        };
        std::mem::size_of::<Self>()
            .checked_add(payload)
            .ok_or(Refusal::Budget)
    }

    pub fn json(&self) -> Json {
        let (kind, value) = match self {
            Self::Null => return json!({"type": "null"}),
            Self::Bool(v) => ("boolean", Json::Bool(*v)),
            Self::Int(v) => ("integer", json!(v.to_string())),
            Self::Float(v) => ("float", json!(format!("{:016x}", v.to_bits()))),
            Self::Decimal(v) => ("decimal", json!(v.to_string())),
            Self::String(v) => ("string", json!(v)),
            Self::Binary(v) => ("binary", json!(crate::binary::format(v))),
            Self::Uuid(v) => ("uuid", json!(crate::uuid::format(*v))),
            Self::Date(v) => ("date", json!(v.to_string())),
            Self::Time(v) => ("time", json!(v.to_string())),
            Self::Timestamp(v) => ("timestamp", json!(v.to_string())),
            Self::Months(v) => ("months", json!(v.to_string())),
            Self::Micros(v) => ("micros", json!(v.to_string())),
            Self::Array(v) => ("array", v.iter().map(Self::json).collect()),
            Self::Map(entries) => (
                "map",
                entries
                    .iter()
                    .map(|(k, v)| json!([k.json(), v.json()]))
                    .collect(),
            ),
            Self::Point(x, y) => (
                "point",
                json!([
                    format!("{:016x}", x.to_bits()),
                    format!("{:016x}", y.to_bits())
                ]),
            ),
        };
        json!({"type": kind, "value": value})
    }
}

#[derive(Debug)]
pub enum Refusal {
    Unsupported(String),
    RuntimeError(&'static str),
    Budget,
}

pub(crate) type Result<T> = std::result::Result<T, Refusal>;

pub(crate) fn unsupported(reason: impl Into<String>) -> Refusal {
    Refusal::Unsupported(reason.into())
}
