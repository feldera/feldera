//! The NULL constructors in DBSPLiteral.none differ in their type identity.
//! Some retain the supplied output type, others construct a fresh or singleton type.
use super::{range, reference, ConstantField, Error, Ids};
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::types::{FloatFlavor, TypeKind};
use serde_json::Value;

pub(super) fn lower(
    ids: &mut Ids,
    kind: &TypeKind,
    scalar: &mut Value,
    output_field: &Value,
) -> Result<ConstantField, Error> {
    let position = range(0, 0, 0, 0);
    let mut literal = match kind {
        TypeKind::Null {} => ids.generated("DBSPNullLiteral"),
        TypeKind::Integer { width, signed } => {
            let prefix = if *signed { "I" } else { "U" };
            ids.generated(&format!("DBSP{prefix}{}Literal", width.bits()))
        }
        TypeKind::Boolean {} => ids.generated("DBSPBoolLiteral"),
        TypeKind::Float {
            flavor: FloatFlavor::Double,
        } => ids.generated("DBSPDoubleLiteral"),
        TypeKind::Float {
            flavor: FloatFlavor::Real,
        } => ids.generated("DBSPRealLiteral"),
        TypeKind::Character { .. } => {
            *scalar = reference(output_field)?;
            let mut literal = ids.generated("DBSPStringLiteral");
            literal["charset"] = "UTF-8".into();
            literal
        }
        TypeKind::Binary { .. } | TypeKind::Decimal { .. } => {
            *scalar = reference(output_field)?;
            let class = if matches!(kind, TypeKind::Binary { .. }) {
                "DBSPBinaryLiteral"
            } else {
                "DBSPDecimalLiteral"
            };
            ids.node(class, &position)
        }
        TypeKind::Date {} => {
            singleton_literal(ids, scalar, CircuitType::Date, "DATE", "DBSPDateLiteral")
        }
        TypeKind::Time { .. } => {
            singleton_literal(ids, scalar, CircuitType::Time, "TIME", "DBSPTimeLiteral")
        }
        TypeKind::Timestamp { .. } => singleton_literal(
            ids,
            scalar,
            CircuitType::Timestamp,
            "TIMESTAMP",
            "DBSPTimestampLiteral",
        ),
        TypeKind::Uuid {} => {
            singleton_literal(ids, scalar, CircuitType::Uuid, "UUID", "DBSPUuidLiteral")
        }
        TypeKind::Variant {} => {
            let mut literal = ids.generated("DBSPVariantExpression");
            literal["isSqlNull"] = false.into();
            literal
        }
        _ => return Err(Error("unsupported typed NULL in VALUES".into())),
    };
    if matches!(kind, TypeKind::Integer { .. } | TypeKind::Boolean {}) {
        scalar
            .as_object_mut()
            .expect("fresh scalar")
            .remove("position");
        scalar["sourceEmpty"] = true.into();
    }
    literal["type"] = reference(scalar)?;
    Ok(ConstantField {
        literal,
        digest: "null".into(),
        sql: "NULL".into(),
    })
}

fn singleton_literal(
    ids: &mut Ids,
    scalar: &mut Value,
    kind: CircuitType,
    code: &str,
    class: &str,
) -> Value {
    *scalar = ids.singleton(kind, code, true);
    ids.generated(class)
}
