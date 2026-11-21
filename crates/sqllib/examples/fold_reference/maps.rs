//! Map folds checked against sqllib's BTreeMap representation and operators.

use feldera_sqllib::*;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

type Entries<K> = Vec<(K, Option<String>)>;

#[derive(Deserialize)]
struct Input<K> {
    maps: Vec<Option<Entries<K>>>,
    key: Option<K>,
}

fn run<K: Ord + Clone + DeserializeOwned>(
    operation: &str,
    input: &str,
    encode: impl Fn(&K) -> Value,
) -> Result<Value, String> {
    let input: Input<K> = serde_json::from_str(input).map_err(|e| e.to_string())?;
    let mut maps = input.maps.into_iter().map(|entries| {
        entries.map(|entries| {
            to_map(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, v.map(SqlString::from)))
                    .collect(),
            )
        })
    });
    let first = maps.next().ok_or("missing map")?;
    let string = |s: Option<SqlString>| {
        s.map_or_else(
            || json!({"type":"null"}),
            |s| json!({"type":"string", "value":s.str()}),
        )
    };
    match operation {
        "contains" => Ok(map_contains_keyNN(first, input.key).map_or_else(
            || json!({"type":"null"}),
            |b| json!({"type":"boolean", "value":b}),
        )),
        "item" => Ok(string(map_indexNNN(&first, input.key))),
        "cardinality" => Ok(cardinalityMapN(first).map_or_else(
            || json!({"type":"null"}),
            |n| json!({"type":"integer", "value":n.to_string()}),
        )),
        "construct" | "concat" => {
            let result = if operation == "concat" {
                maps.fold(first, map_concatNN)
            } else {
                first
            };
            Ok(result.map_or_else(|| json!({"type":"null"}), |map| json!({"type":"map", "value":map.iter().map(|(k,v)| json!([encode(k), string(v.clone())])).collect::<Vec<_>>()})))
        }
        _ => Err("unknown map operation".into()),
    }
}

pub(super) fn evaluate(operation: &str, input: &str) -> Result<Value, String> {
    let (kind, operation) = operation.split_once('/').ok_or("missing map key type")?;
    match kind {
        "BIGINT" => run::<i64>(
            operation,
            input,
            |v| json!({"type":"integer", "value":v.to_string()}),
        ),
        "VARCHAR" => run::<String>(operation, input, |v| json!({"type":"string", "value":v})),
        "ARRAY" => run::<Vec<Option<i64>>>(
            operation,
            input,
            |v| json!({"type":"array", "value":v.iter().map(|n| n.map_or_else(|| json!({"type":"null"}), |n| json!({"type":"integer", "value":n.to_string()}))).collect::<Vec<_>>()}),
        ),
        _ => Err("unknown map key type".into()),
    }
}
