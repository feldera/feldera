//! Selection oracle using sqllib operators and runtime value representations.

use dbsp::algebra::F64;
use feldera_sqllib::*;
use serde_json::{Value, json};
use std::str::FromStr;

fn select<T: Ord + Clone>(operation: &str, values: Vec<Option<T>>) -> Option<T> {
    if operation == "nullif" {
        let [left, right]: [Option<T>; 2] = values.try_into().ok().expect("two NULLIF operands");
        // ExpressionCompiler lowers NULLIF to this nullable comparison and IF.
        return if eq_N_N(left.clone(), right).unwrap_or(false) {
            None
        } else {
            left
        };
    }
    let mut values = values.into_iter();
    let first = values.next().expect("at least one selection operand");
    values.fold(first, |left, right| match operation {
        "least" => min_N_N(left, right),
        "greatest" => max_null_wins_N_N(left, right),
        "least_pg" => min_ignore_nulls_N_N(left, right),
        "greatest_pg" => max_ignore_nulls_N_N(left, right),
        _ => panic!("unknown selection oracle operation {operation}"),
    })
}

fn run<T: Ord + Clone>(
    operation: &str,
    values: Vec<Option<T>>,
    encode: impl Fn(T) -> Value,
) -> Value {
    let encode = |value: Option<T>| value.map_or_else(|| json!({"type":"null"}), &encode);
    if let Some(order) = operation.strip_prefix("sort_") {
        let values = sort_array(std::sync::Arc::new(values), order == "asc");
        json!({"type":"array", "value":values.iter().cloned().map(encode).collect::<Vec<_>>()})
    } else {
        encode(select(operation, values))
    }
}

fn parse<T: FromStr>(values: Vec<Option<String>>) -> Result<Vec<Option<T>>, String>
where
    T::Err: std::fmt::Display,
{
    values
        .into_iter()
        .map(|value| {
            value
                .map(|v| v.parse().map_err(|e: T::Err| e.to_string()))
                .transpose()
        })
        .collect()
}

pub(super) fn evaluate(operation: &str, input: &str) -> Result<Value, String> {
    let (kind, operation) = operation.split_once('/').ok_or("missing selection type")?;
    let values: Vec<Option<String>> = serde_json::from_str(input).map_err(|e| e.to_string())?;
    Ok(match kind {
        "BIGINT" => run(
            operation,
            parse::<i64>(values)?,
            |v| json!({"type":"integer", "value":v.to_string()}),
        ),
        "DECIMAL" => run(
            operation,
            parse::<SqlDecimal<12, 3>>(values)?,
            |v| json!({"type":"decimal", "value":v.to_string()}),
        ),
        "DOUBLE" => run(
            operation,
            parse::<f64>(values)?
                .into_iter()
                .map(|v| v.map(F64::new))
                .collect(),
            |v| json!({"type":"float", "value":format!("{:016x}", v.into_inner().to_bits())}),
        ),
        "VARCHAR" => run(
            operation,
            values.into_iter().map(|v| v.map(SqlString::from)).collect(),
            |v| json!({"type":"string", "value":v.str()}),
        ),
        _ => return Err(format!("unknown selection oracle type {kind}")),
    })
}
