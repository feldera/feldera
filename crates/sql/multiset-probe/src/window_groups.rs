//! Preserve window and field order when grouping compatible window inputs.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    threshold: usize,
    candidates: Vec<Candidate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    window: u64,
    input: String,
    fields: Vec<String>,
}

#[derive(Default, Serialize)]
struct Group {
    members: Vec<Member>,
    shared: Vec<Field>,
}

#[derive(Serialize)]
struct Member {
    window: u64,
    fields: Vec<usize>,
}

#[derive(Serialize)]
struct Field {
    window: u64,
    ordinal: usize,
}

pub(crate) fn group(wire: &Value) -> Result<Value> {
    let request: Request = serde_json::from_value(wire.clone())
        .map_err(|error| Error(format!("invalid window group request: {error}")))?;
    let mut by_input = HashMap::new();
    let mut windows = HashSet::new();
    let mut groups: Vec<Vec<&Candidate>> = Vec::new();
    for candidate in &request.candidates {
        if !windows.insert(candidate.window) {
            return Err(Error(format!("duplicate window {}", candidate.window)));
        }
        let group = *by_input.entry(candidate.input.as_str()).or_insert_with(|| {
            let group = groups.len();
            groups.push(Vec::new());
            group
        });
        groups[group].push(candidate);
    }
    let selected = groups
        .into_iter()
        .filter(|group| request.threshold != 0 && group.len() >= request.threshold)
        .map(build_group)
        .collect::<Vec<_>>();
    serde_json::to_value(selected)
        .map_err(|error| Error(format!("cannot encode window groups: {error}")))
}

fn build_group(candidates: Vec<&Candidate>) -> Group {
    let mut group = Group::default();
    let mut positions = HashMap::new();
    for candidate in candidates {
        let fields = candidate
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                *positions.entry(field.as_str()).or_insert_with(|| {
                    let position = group.shared.len();
                    group.shared.push(Field {
                        window: candidate.window,
                        ordinal,
                    });
                    position
                })
            })
            .collect();
        group.members.push(Member {
            window: candidate.window,
            fields,
        });
    }
    group
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn windows_and_shared_fields_keep_first_use_order() {
        let mut request = json!({"threshold": 2, "candidates": [
            {"window": 9, "input": "same", "fields": ["a", "b", "a"]},
            {"window": 1, "input": "other", "fields": ["a"]},
            {"window": 6, "input": "same", "fields": ["c", "b"]}
        ]});
        assert_eq!(
            group(&request).unwrap(),
            json!([{
                "members": [{"window": 9, "fields": [0, 1, 0]}, {"window": 6, "fields": [2, 1]}],
                "shared": [{"window": 9, "ordinal": 0}, {"window": 9, "ordinal": 1}, {"window": 6, "ordinal": 0}]
            }])
        );
        request["threshold"] = 0.into();
        assert_eq!(group(&request).unwrap(), json!([]));
    }
}
