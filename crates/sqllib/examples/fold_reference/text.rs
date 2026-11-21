//! Text conversions checked through sqllib's nullable runtime entry points.

use feldera_sqllib::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Deserialize)]
struct Join {
    values: Option<Vec<Option<String>>>,
    separator: Option<String>,
    replacement: Option<String>,
}

pub(super) fn evaluate(operation: &str, input: &str) -> Result<Value, String> {
    let result = match operation {
        "join2" | "join3" => {
            let input: Join = serde_json::from_str(input).map_err(|e| e.to_string())?;
            let values = input.values.map(|values| {
                Arc::new(values.into_iter().map(|s| s.map(SqlString::from)).collect())
            });
            let separator = input.separator.map(SqlString::from);
            if operation == "join2" {
                array_to_string2NvecNN(values, separator)
            } else {
                array_to_string3NvecNNN(values, separator, input.replacement.map(SqlString::from))
            }
        }
        "concat" => {
            let values: Vec<Option<String>> =
                serde_json::from_str(input).map_err(|e| e.to_string())?;
            let mut values = values.into_iter().map(|s| s.map(SqlString::from));
            let first = values.next().ok_or("empty CONCAT")?;
            values.fold(first, concat_sN_sN)
        }
        "date" => {
            let (value, size, fixed): (Option<String>, i32, bool) =
                serde_json::from_str(input).map_err(|e| e.to_string())?;
            // The input is already a typed DATE, not a VARCHAR-to-DATE cast.
            let date = value
                .map(|s| {
                    s.parse::<chrono::NaiveDate>()
                        .map(Date::from_date)
                        .map_err(|e| e.to_string())
                })
                .transpose()?;
            cast_to_sN_DateN(date, size, fixed).map_err(|e| e.to_string())?
        }
        _ => return Err("unknown text operation".into()),
    };
    Ok(result.map_or_else(
        || json!({"type":"null"}),
        |s| json!({"type":"string", "value":s.str()}),
    ))
}
