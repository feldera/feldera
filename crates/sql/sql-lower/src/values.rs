//! Lower a single constant row to the constant weighted collection in S2.
use super::{
    projection::expressions,
    source_table::{range, statement_position, stream_type, Ids, RowLayout},
    view::{plan_id, reference, Stream},
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::ObjectKind,
    literals::LiteralValue,
    program::Program,
    relation_properties::Properties,
    statements::{RelRoot, StatementSource},
    types::{FloatFlavor, IntegerWidth, Precision, SqlType, TimeZone, TypeKind},
};
use serde_json::{json, Value};

pub(super) fn lower(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    root: &RelRoot,
    source: &StatementSource,
) -> Result<Stream, Error> {
    let rel = &program.properties.relations[root.relation as usize];
    let roots = expressions(program, root.relation)?;
    if !matches!(rel.fields, Properties::Values { tuples: 1 })
        || !rel.metadata.traits.is_empty()
        || !rel.metadata.hints.is_empty()
        || roots.len() != schema.columns.len()
        || roots.len() != root.fields.len()
    {
        return Err(Error("unsupported VALUES metadata".into()));
    }
    let unknown = range(0, 0, 0, 0);
    let layout = RowLayout::new(schema, &program.table.type_table)?;
    let element = layout.tuple(ids, &unknown, true);
    let element_ref = reference(&element)?;
    let output = stream_type(ids, element, &unknown)?;
    let output_type = reference(&output)?;

    let mut scalars = layout.column_types(ids);
    let mut literals = Vec::new();
    let mut columns = Vec::new();
    let mut values = Vec::new();
    let mut sql_values = Vec::new();
    let mut aliases = Vec::new();
    for (index, &expr_id) in roots.iter().enumerate() {
        let field = &root.fields[index];
        let column = &schema.columns[index];
        let expr = &program.expressions.expressions[expr_id.0 as usize];
        if field.index as usize != index || field.name != column.name.name || expr.ty != column.ty {
            return Err(Error("VALUES column differs from SELECT output".into()));
        }
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected a VALUES literal".into()));
        };
        let ty = &program.table.type_table.types[column.ty.0 as usize];
        let cell = lower_literal(ids, ty, &literal.value, &mut scalars[index])?;
        columns.push(format!("{} {}", cell.type_name, field.name));
        aliases.push(format!("`{}`", field.name.replace('`', "``")));
        values.push(cell.digest);
        sql_values.push(cell.sql);
        literals.push(cell.literal);
    }
    let mut tuple = ids.generated("DBSPTypeTuple");
    tuple["tupFields"] = scalars.into();
    tuple["code"] = "TUPLE".into();
    tuple["mayBeNull"] = false.into();
    let mut row = ids.node("DBSPTupleExpression", &unknown);
    row["type"] = tuple;
    row["fields"] = literals.into();
    let mut zset = ids.generated("DBSPZSetExpression");
    zset["data"] = json!([row]);
    zset["type"] = output_type.clone();
    zset["weights"] = json!([1]);
    zset["elementType"] = element_ref;
    let relation = ids.relation(root.relation);
    let operator = ids.next_operator();
    let plan = plan_id(checkpoint, &schema.name.name)?;
    let columns = columns.join(", ");
    let text = values.join(", ");
    let sql = sql_values.join(", ");
    let aliases = aliases.join(", ");
    let origin = json!({"id":relation,"relId":relation,"planId":plan,"tableScan":false,
        "digest":format!("LogicalValues.(type=RecordType:peek_no_expand({columns}),tuples=[{{ {text} }}])"),
        "text":format!("LogicalValues(tuples=[[{{ {text} }}]])"),
        "sql":format!("SELECT *\nFROM (VALUES ({sql})) AS `t` ({aliases})"),"statefulSqlName":null,
        "origin":{"name":schema.name.name,"caseSensitive":schema.name.quoted,"position":statement_position(source)}});
    let op = json!({"class":"DBSPConstantOperator","id":operator,"annotations":[],"inputs":[],
        "provenance":{"id":ids.next_origin(),"kind":"LastRel","position":unknown,"positions":[],"relation":origin},
        "outputType":output,"function":zset});
    checkpoint["circuit"]["allOperators"]
        .as_array_mut()
        .expect("system operators")
        .push(op);
    Ok(Stream {
        origin,
        operator: operator.into(),
        output_type,
        relation: json!({"ref":relation}),
    })
}

struct ConstantField {
    literal: Value,
    type_name: String,
    digest: String,
    sql: String,
}

fn lower_literal(
    ids: &mut Ids,
    ty: &SqlType,
    value: &LiteralValue,
    scalar: &mut Value,
) -> Result<ConstantField, Error> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let unknown = range(0, 0, 0, 0);
    let (mut literal, type_name, digest, sql) = match (&ty.kind, value) {
        (TypeKind::Null {}, LiteralValue::Null {}) => (
            ids.generated("DBSPNullLiteral"),
            "NULL".into(),
            "null".into(),
            "NULL".into(),
        ),
        (_, _) if ty.nullable => {
            return Err(Error("unsupported nullable VALUES literal type".into()));
        }
        (TypeKind::Uuid {}, LiteralValue::Uuid { bytes }) => {
            let text = feldera_sql_rel_model::literals::uuid_to_text(*bytes);
            let mut literal = ids.node("DBSPUuidLiteral", &unknown);
            literal["value"] = text.clone().into();
            let sql = format!("UUID '{text}'");
            (literal, "UUID".into(), text, sql)
        }
        (TypeKind::Date {}, LiteralValue::Date { epoch_days }) => {
            let text = feldera_sql_rel_model::literals::date_to_text(*epoch_days)
                .ok_or_else(|| Error("DATE literal is outside the SQL year range".into()))?;
            let mut literal = ids.node("DBSPDateLiteral", &unknown);
            literal["value"] = (*epoch_days).into();
            let sql = format!("DATE '{text}'");
            (literal, "DATE".into(), text, sql)
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
            let mut literal = ids.node("DBSPTimeLiteral", &unknown);
            literal["value"] = digest.clone().into();
            (
                literal,
                format!("TIME({p})"),
                digest,
                format!("TIME '{time}'"),
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
            let date = feldera_sql_rel_model::literals::date_to_text(*epoch_days)
                .ok_or_else(|| Error("timestamp outside the SQL date range".into()))?;
            let (time, digest) = clock_text(*nanoseconds_since_midnight, *p)?;
            let micros = i64::from(*epoch_days) * 86_400_000_000
                + (nanoseconds_since_midnight / 1000) as i64;
            let mut literal = ids.node("DBSPTimestampLiteral", &unknown);
            literal["value"] = micros.to_string().into();
            (
                literal,
                format!("TIMESTAMP({p})"),
                format!("{date} {digest}"),
                format!("TIMESTAMP '{date} {time}'"),
            )
        }
        (
            TypeKind::Decimal {
                precision, scale, ..
            },
            LiteralValue::Decimal { value },
        ) => {
            let (sql, digest) = value
                .sql_and_digest()
                .ok_or_else(|| Error("unsupported SQL decimal literal".into()))?;
            let mut literal = ids.node("DBSPDecimalLiteral", &unknown);
            literal["value"] = digest.clone().into();
            (
                literal,
                format!("DECIMAL({precision}, {scale})"),
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
        ) => {
            let (class, name) = match width {
                IntegerWidth::I8 => ("DBSPI8Literal", "TINYINT"),
                IntegerWidth::I16 => ("DBSPI16Literal", "SMALLINT"),
                IntegerWidth::I32 => ("DBSPI32Literal", "INTEGER"),
                IntegerWidth::I64 => ("DBSPI64Literal", "BIGINT"),
            };
            let mut literal = ids.generated(class);
            literal["value"] = value.clone().into();
            (literal, name.into(), value.clone(), value.clone())
        }
        (TypeKind::Boolean {}, LiteralValue::Boolean { value }) => {
            let mut literal = ids.generated("DBSPBoolLiteral");
            literal["value"] = (*value).into();
            (
                literal,
                "BOOLEAN".into(),
                value.to_string(),
                value.to_string().to_uppercase(),
            )
        }
        (
            TypeKind::Character {
                fixed: true,
                length: Precision::Specified(n),
                metadata,
            },
            LiteralValue::Character { value },
        ) if *metadata == feldera_sql_rel_model::types::CharacterMetadata::implicit() => {
            let mut literal = ids.generated("DBSPStringLiteral");
            literal["value"] = value.clone().into();
            literal["charset"] = "UTF-8".into();
            let text = format!("_UTF-8'{}'", value.replace('\'', "''"));
            (literal, format!("CHAR({n})"), text.clone(), text)
        }
        (
            TypeKind::Binary {
                fixed: true,
                length: Precision::Specified(n),
            },
            LiteralValue::Binary { value },
        ) => {
            let mut literal = ids.node("DBSPBinaryLiteral", &unknown);
            literal["value"] = STANDARD.encode(value).into();
            let digest = format!("X'{}'", hex::encode(value));
            let sql = digest.to_uppercase();
            (literal, format!("BINARY({n})"), digest, sql)
        }
        (
            TypeKind::Float {
                flavor: FloatFlavor::Double,
            },
            LiteralValue::Float64 { bits },
        ) => {
            let n = f64::from_bits(*bits);
            if !n.is_finite() {
                return Err(Error("nonfinite VALUES double".into()));
            }
            let mut literal = ids.generated("DBSPDoubleLiteral");
            literal["value"] = (*bits as i64).into();
            let (digest, sql) = double_spelling(n);
            (literal, "DOUBLE".into(), digest, sql)
        }
        _ => return Err(Error("unsupported scalar VALUES element".into())),
    };
    // Integral and boolean constant constructors create types without a source
    // object. String and binary literals use types associated with the relation.
    if matches!(ty.kind, TypeKind::Integer { .. } | TypeKind::Boolean {}) {
        scalar
            .as_object_mut()
            .expect("scalar type")
            .remove("position");
        scalar["sourceEmpty"] = true.into();
    }
    literal["type"] = reference(scalar)?;
    Ok(ConstantField {
        literal,
        type_name,
        digest,
        sql,
    })
}

/// The plan's SQL uses the declared precision. The relational digest and
/// the DBSP time literal drop trailing fractional zeros.
fn clock_text(nanos: u64, precision: u32) -> Result<(String, String), Error> {
    if precision > 9 || nanos >= 86_400_000_000_000 {
        return Err(Error("invalid SQL time value or precision".into()));
    }
    let seconds = nanos / 1_000_000_000;
    let mut sql = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if precision == 0 {
        return Ok((sql.clone(), sql));
    }
    let fraction = format!("{:09}", nanos % 1_000_000_000);
    sql.push('.');
    sql.push_str(&fraction[..precision as usize]);
    let digest = sql.trim_end_matches('0').trim_end_matches('.').to_owned();
    Ok((sql, digest))
}

// The relational digest uses Calcite's Double spelling with an explicit
// exponent. SQL rendering goes through its BigDecimal scientific formatter.
fn double_spelling(n: f64) -> (String, String) {
    let scientific = format!("{n:e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific float");
    let exponent: i32 = exponent.parse().expect("finite float exponent");
    let sql = format!("{mantissa}E{exponent}");
    let (mut digits, power) = if n == 0.0 || (-3..7).contains(&exponent) {
        (n.to_string(), 0)
    } else {
        (mantissa.to_string(), exponent)
    };
    if !digits.contains('.') {
        digits.push_str(".0");
    }
    (format!("{digits}E{power}"), sql)
}
