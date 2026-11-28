//! Scalar literal spellings and circuit constructors shared by VALUES and projections.

use super::{double_spelling, null, range, reference, temporal::TemporalLiteral, Error, Ids};
use base64::{engine::general_purpose::STANDARD, Engine};
use feldera_sql_rel_model::{
    literals::LiteralValue,
    types::{CharacterMetadata, FloatFlavor, Precision, SqlType, TypeKind},
};
use serde_json::Value;

pub(crate) struct ScalarLiteral {
    pub(crate) digest: String,
    pub(crate) sql: String,
    payload: Payload,
}

enum Payload {
    Null,
    Temporal(TemporalLiteral),
    Value {
        class: String,
        value: Value,
        generated: bool,
    },
}

impl ScalarLiteral {
    pub(crate) fn resolve(ty: &SqlType, value: &LiteralValue) -> Result<Self, Error> {
        let payload = |class: &str, value: Value, generated: bool| Payload::Value {
            class: class.into(),
            value,
            generated,
        };
        let (payload, digest, sql) = match (&ty.kind, value) {
            (_, LiteralValue::Null {}) if ty.nullable => {
                (Payload::Null, "null".into(), "NULL".into())
            }
            (_, _) if ty.nullable => {
                return Err(Error(
                    "unsupported non-null literal of nullable type".into(),
                ))
            }
            (TypeKind::Uuid {}, LiteralValue::Uuid { bytes }) => {
                let text = feldera_sql_rel_model::literals::uuid_to_text(*bytes);
                let sql = format!("UUID '{text}'");
                (
                    payload("DBSPUuidLiteral", text.clone().into(), false),
                    text,
                    sql,
                )
            }
            (TypeKind::Date {} | TypeKind::Time { .. } | TypeKind::Timestamp { .. }, _) => {
                let lit = TemporalLiteral::resolve(&ty.kind, value)?;
                let (digest, sql) = (lit.digest.clone(), lit.sql.clone());
                (Payload::Temporal(lit), digest, sql)
            }
            (TypeKind::Decimal { .. }, LiteralValue::Decimal { value }) => {
                let (sql, digest) = value
                    .sql_and_digest()
                    .ok_or_else(|| Error("unsupported SQL decimal literal".into()))?;
                (
                    payload("DBSPDecimalLiteral", digest.clone().into(), false),
                    digest,
                    sql,
                )
            }
            (
                TypeKind::Integer {
                    width,
                    signed: true,
                },
                LiteralValue::Integer { value },
            ) => (
                payload(
                    &format!("DBSPI{}Literal", width.bits()),
                    value.as_str().into(),
                    true,
                ),
                value.clone(),
                value.clone(),
            ),
            (TypeKind::Boolean {}, LiteralValue::Boolean { value }) => (
                payload("DBSPBoolLiteral", (*value).into(), true),
                value.to_string(),
                value.to_string().to_uppercase(),
            ),
            (
                TypeKind::Character {
                    length: Precision::Specified(_),
                    metadata,
                    ..
                },
                LiteralValue::Character { value },
            ) if *metadata == CharacterMetadata::implicit() => {
                let text = format!("_UTF-8'{}'", value.replace('\'', "''"));
                (
                    payload("DBSPStringLiteral", value.as_str().into(), true),
                    text.clone(),
                    text,
                )
            }
            (
                TypeKind::Binary {
                    length: Precision::Specified(_),
                    ..
                },
                LiteralValue::Binary { value },
            ) => {
                let digest = format!("X'{}'", hex::encode(value));
                let sql = digest.to_uppercase();
                (
                    payload("DBSPBinaryLiteral", STANDARD.encode(value).into(), false),
                    digest,
                    sql,
                )
            }
            (
                TypeKind::Float {
                    flavor: FloatFlavor::Double,
                },
                LiteralValue::Float64 { bits },
            ) => {
                let (digest, sql) = double_spelling(f64::from_bits(*bits));
                (
                    payload("DBSPDoubleLiteral", (*bits as i64).into(), true),
                    digest,
                    sql,
                )
            }
            _ => return Err(Error("unsupported scalar literal".into())),
        };
        Ok(Self {
            digest,
            sql,
            payload,
        })
    }

    /// VALUES literals can share a type with their tuple. A projected literal
    /// instead owns the type node returned in its expression.
    pub(crate) fn expression(
        &self,
        ids: &mut Ids,
        ty: &SqlType,
        scalar: &mut Value,
        output_field: Option<&Value>,
    ) -> Result<Value, Error> {
        let mut literal = match &self.payload {
            Payload::Null => null::lower(ids, &ty.kind, scalar, output_field)?,
            Payload::Temporal(lit) => lit.expression(ids),
            Payload::Value {
                class,
                value,
                generated,
            } => {
                let mut node = if *generated {
                    ids.generated(class)
                } else {
                    ids.node(class, &range(0, 0, 0, 0))
                };
                node["value"] = value.clone();
                if matches!(ty.kind, TypeKind::Character { .. }) {
                    node["charset"] = "UTF-8".into();
                }
                node
            }
        };
        if matches!(ty.kind, TypeKind::Integer { .. } | TypeKind::Boolean {}) {
            scalar
                .as_object_mut()
                .expect("fresh scalar type")
                .remove("position");
            scalar["sourceEmpty"] = true.into();
        }
        literal["type"] = if output_field.is_some() {
            reference(scalar)?
        } else {
            scalar.take()
        };
        Ok(literal)
    }
}
