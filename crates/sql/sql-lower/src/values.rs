//! Lower empty and single-row VALUES relations to weighted collections.

mod null;
pub(super) mod scalar;
pub(super) mod temporal;
pub(super) mod type_name;

use super::{
    projection::expressions,
    source_table::{range, statement_position, stream_type, Ids, RowLayout},
    view::{reference, Stream},
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema, expression_objects::ObjectKind, program::Program,
    relation_properties::Properties, statements::StatementSource,
};
use serde_json::{json, Value};

pub(super) fn lower(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    relation_id: u32,
    source: &StatementSource,
) -> Result<Stream, Error> {
    let rel = &program.properties.relations[relation_id as usize];
    let roots = expressions(program, relation_id)?;
    let Properties::Values { tuples } = rel.fields else {
        return Err(Error("expected a VALUES relation".into()));
    };
    let empty = tuples == 0;
    let expected_cells = if empty { 0 } else { schema.columns.len() };
    if tuples > 1
        || !rel.metadata.traits.is_empty()
        || !rel.metadata.hints.is_empty()
        || roots.len() != expected_cells
    {
        return Err(Error("unsupported VALUES metadata".into()));
    }
    let unknown = range(0, 0, 0, 0);
    let layout = RowLayout::new(schema, &program.table.type_table)?;
    let element = layout.tuple(ids, &unknown, true);
    let element_ref = reference(&element)?;
    let output = stream_type(ids, element, &unknown)?;
    let output_type = reference(&output)?;

    let row = if empty {
        None
    } else {
        Some(lower_row(
            program,
            ids,
            roots,
            schema,
            &layout,
            &output["elementType"]["tupFields"],
        )?)
    };
    let aliases = schema
        .columns
        .iter()
        .map(|field| format!("`{}`", field.name.name.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(", ");
    let (data, tuples_text, sql) = match row {
        Some(row) => (
            vec![row.tuple],
            format!("{{ {} }}", row.digest),
            format!("SELECT *\nFROM (VALUES ({})) AS `t` ({aliases})", row.sql),
        ),
        None => {
            let nulls = vec!["NULL"; schema.columns.len()].join(", ");
            (
                vec![],
                String::new(),
                format!("SELECT *\nFROM (VALUES ({nulls})) AS `t` ({aliases})\nWHERE 1 = 0"),
            )
        }
    };
    let mut zset = ids.generated("DBSPZSetExpression");
    zset["weights"] = vec![1; data.len()].into();
    zset["data"] = data.into();
    zset["type"] = output_type.clone();
    zset["elementType"] = element_ref;
    let relation = ids.relation(relation_id);
    let operator = ids.next_operator();
    let plan = ids.plan(relation_id)?;
    let row = program.producers.relations[relation_id as usize].row;
    let row_name = type_name::render(&program.table.type_table, row)?;
    let origin = json!({"id":relation,"relId":relation,"planId":plan,"tableScan":false,
        "digest":format!("LogicalValues.(type={row_name},tuples=[{tuples_text}])"),
        "text":format!("LogicalValues(tuples=[[{tuples_text}]])"),
        "sql":sql,"statefulSqlName":null,
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

struct ConstantRow {
    tuple: Value,
    digest: String,
    sql: String,
}

fn lower_row(
    program: &Program,
    ids: &mut Ids,
    roots: &[feldera_sql_rel_model::expressions::ExprId],
    schema: &CatalogSchema,
    layout: &RowLayout<'_>,
    element_fields: &Value,
) -> Result<ConstantRow, Error> {
    let mut scalars = layout.column_types(ids);
    let mut fields = Vec::with_capacity(roots.len());
    let mut values = Vec::with_capacity(roots.len());
    let mut sql_values = Vec::with_capacity(roots.len());
    for (index, ((&expr_id, column), scalar)) in roots
        .iter()
        .zip(&schema.columns)
        .zip(&mut scalars)
        .enumerate()
    {
        let expr = &program.expressions.expressions[expr_id.0 as usize];
        if expr.ty != column.ty {
            return Err(Error("VALUES column differs from SELECT output".into()));
        }
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected a VALUES literal".into()));
        };
        let ty = &program.table.type_table.types[column.ty.0 as usize];
        let cell = scalar::ScalarLiteral::resolve(ty, &literal.value)?;
        fields.push(cell.expression(ids, ty, scalar, Some(&element_fields[index]))?);
        values.push(cell.digest);
        sql_values.push(cell.sql);
    }
    let mut tuple = ids.generated("DBSPTypeTuple");
    tuple["tupFields"] = scalars.into();
    tuple["code"] = "TUPLE".into();
    tuple["mayBeNull"] = false.into();
    let mut row = ids.node("DBSPTupleExpression", &range(0, 0, 0, 0));
    row["type"] = tuple;
    row["fields"] = fields.into();
    Ok(ConstantRow {
        tuple: row,
        digest: values.join(", "),
        sql: sql_values.join(", "),
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
pub(super) fn double_spelling(n: f64) -> (String, String) {
    if !n.is_finite() {
        let spelling = if n.is_nan() {
            "NaN"
        } else if n.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        };
        return (spelling.into(), format!("CAST('{spelling}' AS DOUBLE)"));
    }
    let scientific = format!("{n:e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific float");
    let exponent: i32 = exponent.parse().expect("finite float exponent");
    let (mut digits, power) = if n == 0.0 || (-3..7).contains(&exponent) {
        (n.to_string(), 0)
    } else {
        (mantissa.to_string(), exponent)
    };
    if !digits.contains('.') {
        digits.push_str(".0");
    }
    // Calcite normalizes BigDecimal.valueOf(double) to scientific notation.
    // Its scale is observable: 10.0 has three significant digits, 1.00E1.
    let sql = if n == 0.0 {
        "0E0".into()
    } else {
        let decimal = digits.trim_start_matches('-').replace('.', "");
        let (first, rest) = decimal.trim_start_matches('0').split_at(1);
        let sign = if n < 0.0 { "-" } else { "" };
        let point = if rest.is_empty() { "" } else { "." };
        format!("{sign}{first}{point}{rest}E{exponent}")
    };
    (format!("{digits}E{power}"), sql)
}
