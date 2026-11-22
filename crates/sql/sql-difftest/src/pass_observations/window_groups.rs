//! WindowGroupOracle records live node IDs. Compare the references in the
//! circuit that owns them so independent JVMs can allocate different IDs.
use super::Invocation;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    window: u64,
    key: String,
    fields: Vec<String>,
    expressions: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    expressions: Vec<[u64; 2]>,
    members: Vec<Member>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    window: u64,
    fields: Vec<usize>,
}

fn port(operator: u64, number: usize) -> Value {
    json!({"operator": {"node": operator}, "outputNumber": number})
}

fn expressions(pairs: Vec<[u64; 2]>) -> Value {
    Value::Array(pairs.into_iter().map(|[body, parameter]| {
        json!({"body": {"node": body}, "parameter": {"node": parameter}})
    }).collect())
}

fn take<T: DeserializeOwned>(invocation: &mut Invocation, name: &str) -> Result<T, String> {
    let value = invocation
        .placement
        .state
        .remove(name)
        .ok_or_else(|| format!("missing {name}"))?;
    serde_json::from_value(value).map_err(|e| format!("invalid {name}: {e}"))
}

pub(super) fn attach(invocation: &mut Invocation) -> Result<(), String> {
    let candidates: Vec<Candidate> = take(invocation, "window_candidates")?;
    let groups: Vec<Group> = take(invocation, "window_groups")?;
    let mut inputs = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        // inputFingerprint prefixes the printed closure with the upstream port.
        let (source, closure) = candidate
            .key
            .split_once('|')
            .ok_or("missing window key closure")?;
        let (operator, number) = source.split_once(':').ok_or("missing window key port")?;
        let source = port(
            operator
                .parse()
                .map_err(|_| "invalid window key operator")?,
            number.parse().map_err(|_| "invalid window key output")?,
        );
        inputs.push(json!({"window": port(candidate.window, 0), "source": source,
            "key": closure, "fields": candidate.fields, "expressions": expressions(candidate.expressions)}));
    }
    let groups: Vec<_> = groups
        .into_iter()
        .map(|group| {
            let members: Vec<_> = group
                .members
                .into_iter()
                .map(|member| json!({"window": port(member.window, 0), "fields": member.fields}))
                .collect();
            json!({"expressions": expressions(group.expressions), "members": members})
        })
        .collect();
    if invocation.after.get("window_selection").is_some() {
        return Err("exit graph already has a window_selection field".into());
    }
    invocation.after["window_selection"] = json!({"candidates": inputs, "groups": groups});
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pass_observations::compare_invocation;

    fn observed(window: u64, field: u64, selected: u64) -> Invocation {
        let circuit = json!({"class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPWindowOperator","id":window,"inputs":[],
                "function":{"class":"DBSPClosureExpression","id":field+3,
                    "parameters":[{"class":"DBSPParameter","id":field+2}],
                    "body":{"class":"DBSPTupleExpression","id":field+4,"fields":[
                        {"class":"DBSPFieldExpression","id":field},
                        {"class":"DBSPFieldExpression","id":field+1}]}}}]});
        serde_json::from_value(json!({
            "container":"ShareWindowIntegrals","index":0,"iteration":0,"instance":0,
            "names_before":0,"names_after":0,"before":circuit,"after":circuit,"diagnostics":[],
            "window_candidates":[{"window":window,"key":format!("{window}:0|(|p0| p0.0)"),
                "fields":["p0.0"],"expressions":[[field,field+2]]}],
            "window_groups":[{"expressions":[[selected,field+2]],"members":[{"window":window,"fields":[0]}]}]
        })).unwrap()
    }

    #[test]
    fn shared_fields_must_reference_the_same_expression_after_renumbering() {
        let compare = |selected| {
            compare_invocation(
                "FindWindowGroups",
                0,
                observed(8, 10, 10),
                observed(23, 40, selected),
            )
        };
        assert!(compare(40).is_ok());
        assert!(compare(41).is_err());
        assert!(compare(99).is_err());
    }
}
