//! Lower a single scalar row to the constant weighted collection in S2.
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
    types::{FloatFlavor, IntegerWidth, TypeKind},
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
    let ([expr], [column], [field]) = (roots, schema.columns.as_slice(), root.fields.as_slice())
    else {
        return Err(Error("expected a single VALUES column".into()));
    };
    if !matches!(rel.fields, Properties::Values { tuples: 1 })
        || !rel.metadata.traits.is_empty()
        || !rel.metadata.hints.is_empty()
        || field.index != 0
        || field.name != column.name.name
    {
        return Err(Error("unsupported VALUES metadata".into()));
    }
    let expr = &program.expressions.expressions[expr.0 as usize];
    let ObjectKind::Literal { literal } = &expr.kind else {
        return Err(Error("expected a VALUES literal".into()));
    };
    let ty = &program.table.type_table.types[column.ty.0 as usize];
    if ty.nullable || expr.ty != column.ty {
        return Err(Error(
            "unsupported nullable or coerced VALUES element".into(),
        ));
    }
    let unknown = range(0, 0, 0, 0);
    let element = RowLayout::new(schema, &program.table.type_table)?.tuple(ids, &unknown, true);
    let element_ref = reference(&element)?;
    let output = stream_type(ids, element, &unknown)?;
    let output_type = reference(&output)?;
    let (typename, text, sql, scalar_type, literal) = match (&ty.kind, &literal.value) {
        (
            TypeKind::Integer {
                width: IntegerWidth::I32,
                signed: true,
            },
            LiteralValue::Integer { value },
        ) => {
            let n = value
                .parse::<i32>()
                .map_err(|_| Error("VALUES integer out of range".into()))?;
            let mut int = ids.generated("DBSPTypeInteger");
            int["width"] = 32.into();
            int["code"] = "INT32".into();
            int["signed"] = true.into();
            int["mayBeNull"] = false.into();
            let mut literal = ids.generated("DBSPI32Literal");
            literal["type"] = reference(&int)?;
            literal["value"] = n.to_string().into();
            ("INTEGER", n.to_string(), n.to_string(), int, literal)
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
            let scalar = reference(&output["elementType"]["tupFields"][0])?;
            literal["type"] = scalar.clone();
            literal["value"] = (*bits as i64).into();
            let (text, sql) = double_spelling(n);
            ("DOUBLE", text, sql, scalar, literal)
        }
        _ => return Err(Error("unsupported scalar VALUES element".into())),
    };
    let mut tuple = ids.generated("DBSPTypeTuple");
    tuple["tupFields"] = json!([scalar_type]);
    tuple["code"] = "TUPLE".into();
    tuple["mayBeNull"] = false.into();
    let mut row = ids.node("DBSPTupleExpression", &unknown);
    row["type"] = tuple;
    row["fields"] = json!([literal]);
    let mut zset = ids.generated("DBSPZSetExpression");
    zset["data"] = json!([row]);
    zset["type"] = output_type.clone();
    zset["weights"] = json!([1]);
    zset["elementType"] = element_ref;
    let relation = ids.next_relation();
    let operator = ids.next_operator();
    let plan = plan_id(checkpoint, &schema.name.name)?;
    let name = &column.name.name;
    let quoted_name = name.replace('`', "``");
    let origin = json!({"id":relation,"relId":relation,"planId":plan,"tableScan":false,
        "digest":format!("LogicalValues.(type=RecordType:peek_no_expand({typename} {name}),tuples=[{{ {text} }}])"),
        "text":format!("LogicalValues(tuples=[[{{ {text} }}]])"),
        "sql":format!("SELECT *\nFROM (VALUES ({sql})) AS `t` (`{quoted_name}`)"),"statefulSqlName":null,
        "origin":{"name":schema.name.name,"caseSensitive":schema.name.quoted,"position":statement_position(source)}});
    let op = json!({"class":"DBSPConstantOperator","id":operator,"annotations":[],"inputs":[],
        "provenance":{"id":ids.next_origin(),"kind":"LastRel","position":unknown,"positions":[],"relation":origin},
        "outputType":output,"function":zset});
    checkpoint["circuit"]["allOperators"]
        .as_array_mut()
        .expect("system operators")
        .push(op);
    Ok(Stream {
        operator: operator.into(),
        output_type,
        relation: json!({"ref":relation}),
    })
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
