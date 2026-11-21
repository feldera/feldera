use dbsp::utils::Tup2;
use serde_json::Value;
use std::str::FromStr;

pub(super) fn class<'a>(node: &'a Value, expected: &str) -> &'a Value {
    assert_eq!(node["class"], expected);
    node
}

pub(super) fn items(node: &Value) -> &[Value] {
    node.as_array().expect("IR list")
}

pub(super) fn one(node: &Value) -> &Value {
    let [value] = items(node) else {
        panic!("expected one item")
    };
    value
}

pub(super) fn links(consumer: &Value, producers: &[&Value]) {
    let inputs = items(&consumer["inputs"]);
    assert_eq!(inputs.len(), producers.len());
    for (input, producer) in inputs.iter().zip(producers) {
        assert_eq!(input["outputNumber"], 0);
        assert_eq!(input["operator"]["node"], producer["id"]);
    }
}

pub(super) fn field(node: &Value, parameter: &Value) -> usize {
    let field = class(node, "DBSPFieldExpression");
    let dereference = class(&field["expression"], "DBSPDerefExpression");
    assert_eq!(
        class(&dereference["expression"], "DBSPVariablePath")["variable"],
        parameter["name"]
    );
    usize::try_from(field["fieldNo"].as_u64().expect("field index")).unwrap()
}

pub(super) fn body(operator: &Value) -> (&[Value], &Value) {
    let closure = class(&operator["function"], "DBSPClosureExpression");
    (items(&closure["parameters"]), &closure["body"])
}

pub(super) fn number<T: FromStr>(node: &Value, name: &str) -> Option<T> {
    class(node, name)["value"]
        .as_str()
        .map(|text| text.parse().ok().expect("typed number"))
}

pub(super) fn bag<T>(
    entry: &Value,
    name: &str,
    decode: impl Fn(&[Value]) -> T,
) -> Vec<Tup2<T, i64>> {
    assert_eq!(entry["name"]["text"], name);
    let zset = class(&entry["zset_ir"], "DBSPZSetExpression");
    let data = items(&zset["data"]);
    let weights = items(&zset["weights"]);
    assert_eq!(data.len(), weights.len());
    data.iter()
        .zip(weights)
        .map(|(row, weight)| {
            Tup2(
                decode(items(&class(row, "DBSPTupleExpression")["fields"])),
                weight.as_i64().unwrap(),
            )
        })
        .collect()
}
