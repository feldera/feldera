use feldera_sql_ir::{
    provenance::SourceRange, BodyOrder, Circuit, Error, Field, OperatorKind, OutputPort,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
struct Hint<'a> {
    input: usize,
    strategy: &'a str,
    #[serde(rename = "inputName")]
    input_name: &'a str,
    #[serde(default)]
    position: SourceRange,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct HintDiagnostic {
    pub range: SourceRange,
    pub continuation: bool,
    pub warning: bool,
    pub error_type: &'static str,
    pub message: String,
}

/// Check all hints on each producer port, including hints from separate consuming joins.
pub fn check_hints(circuit: &Circuit) -> Result<Vec<HintDiagnostic>, Error> {
    use OperatorKind::*;
    let mut by_port: BTreeMap<OutputPort, Vec<Hint<'_>>> = BTreeMap::new();
    let mut reported = BTreeSet::new();
    let mut messages = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let op = circuit.node(id).expect("visited operator");
        if !matches!(
            op.operator_kind(),
            Some(
                Join | JoinIndex
                    | LeftJoin
                    | LeftJoinIndex
                    | JoinFilterMap
                    | LeftJoinFilterMap
                    | AsofJoin
                    | ConcreteAsofJoin
                    | StreamJoin
                    | StreamJoinIndex
            )
        ) {
            continue;
        }
        let Some(Field::Data(annotations)) = op.field("annotations") else {
            continue;
        };
        let Some(annotations) = annotations.as_array() else {
            return Err(Error::new("invalid join annotations"));
        };
        for (index, port) in op.inputs().enumerate() {
            let previous = by_port.entry(port).or_default();
            for annotation in annotations.iter().filter(|a| {
                a["class"] == "JoinStrategy" && a["input"].as_u64() == Some(index as u64)
            }) {
                let hint = Hint::deserialize(annotation).map_err(|e| Error::new(e.to_string()))?;
                for other in previous
                    .iter()
                    .filter(|p| p.input != hint.input || p.strategy != hint.strategy)
                {
                    for (item, continuation, prefix) in [
                        (
                            &hint,
                            false,
                            "Found two conflicting hints for the same collection",
                        ),
                        (other, true, "Previous hint"),
                    ] {
                        let message = format!("{prefix}: {}({})", item.strategy, item.input_name);
                        if reported.insert((item.position, continuation, message.clone())) {
                            messages.push(HintDiagnostic {
                                range: item.position,
                                warning: false,
                                error_type: "Incompatible hints",
                                continuation,
                                message,
                            });
                        }
                    }
                }
                previous.push(hint);
            }
        }
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // CheckHints sees both inputs of the self-join in
    // SELECT /*+ broadcast(T), shard(S) */ * FROM T JOIN T AS S USING (x).
    #[test]
    fn self_join_reports_both_source_ranges_in_java_order() {
        let range = |start_column, end_column| SourceRange {
            start_line_number: 1,
            start_column,
            end_line_number: 1,
            end_column,
        };
        let broadcast = range(48, 63);
        let shard = range(64, 73);
        let port = json!({"outputNumber": 0, "operator": {"node": 191}});
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPConstantOperator","id":191,"inputs":[]},
            {"class":"DBSPJoinOperator","id":192,"inputs":[port,port],"annotations":[
                {"class":"JoinStrategy","strategy":"Broadcast","input":0,"inputName":"t","position":broadcast},
                {"class":"JoinStrategy","strategy":"Shard","input":1,"inputName":"s","position":shard}
            ]}
        ]});
        let hints = check_hints(&Circuit::from_json(graph).unwrap()).unwrap();
        let diagnostics: Vec<_> = hints
            .iter()
            .map(|hint| {
                (
                    hint.range,
                    hint.warning,
                    hint.continuation,
                    hint.error_type,
                    hint.message.as_str(),
                )
            })
            .collect();
        assert_eq!(
            diagnostics,
            [
                (
                    shard,
                    false,
                    false,
                    "Incompatible hints",
                    "Found two conflicting hints for the same collection: Shard(s)"
                ),
                (
                    broadcast,
                    false,
                    true,
                    "Incompatible hints",
                    "Previous hint: Broadcast(t)"
                ),
            ]
        );
    }
}
