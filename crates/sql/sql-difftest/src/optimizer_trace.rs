//! Aggregate the optional Java optimizer journal without counting an attempt as
//! a successful rule production. Reports keep steps and optimization levels apart.

use crate::files::Error;
use feldera_sql_difftest::parse_json;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

#[derive(Default)]
struct Counts {
    invocations: u64,
    incomplete: u64,
    configured: BTreeSet<String>,
    attempts: BTreeMap<String, u64>,
    productions: BTreeMap<String, u64>,
    correlates_before: u64,
    correlates_after: u64,
}

fn add_counts(value: &Value, target: &mut BTreeMap<String, u64>) -> Result<(), Error> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::Difference("expected rule counts".into()))?;
    for (name, count) in object {
        let count = count
            .as_u64()
            .ok_or_else(|| Error::Difference("invalid rule count".into()))?;
        let total = target.entry(name.clone()).or_default();
        *total = total
            .checked_add(count)
            .ok_or_else(|| Error::Difference("rule count overflow".into()))?;
    }
    Ok(())
}

pub fn summarize(path: &Path) -> Result<Value, Error> {
    let input = File::open(path).map_err(|e| Error::Input(e.to_string()))?;
    summarize_records(BufReader::new(input))
        .map_err(|e| Error::Difference(format!("{}: {e}", path.display())))
}

fn summarize_records(input: impl BufRead) -> Result<Value, Error> {
    let mut steps: BTreeMap<(String, u64), Counts> = BTreeMap::new();
    for (index, line) in input.lines().enumerate() {
        let line = line.map_err(|e| Error::Input(e.to_string()))?;
        let invalid = || {
            Error::Difference(format!(
                "line {}: invalid optimizer trace record",
                index + 1
            ))
        };
        let row = parse_json(line.as_bytes()).map_err(|_| invalid())?;
        if row["schema_version"] != 1 {
            return Err(invalid());
        }
        let step = row["step"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(invalid)?;
        let level = row["optimization_level"].as_u64().ok_or_else(invalid)?;
        let completed = row["completed"].as_bool().ok_or_else(invalid)?;
        let before = row
            .pointer("/before/correlates")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        let after = row
            .pointer("/after/correlates")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        let counts = steps.entry((step.to_owned(), level)).or_default();
        counts.invocations += 1;
        counts.incomplete += u64::from(!completed);
        counts.correlates_before += before.len() as u64;
        counts.correlates_after += after.len() as u64;
        // Non-HEP steps have no configured-rule field.
        if let Some(rules) = row.get("configured_rules") {
            for rule in rules.as_array().ok_or_else(invalid)? {
                counts
                    .configured
                    .insert(rule.as_str().ok_or_else(invalid)?.to_owned());
            }
        }
        add_counts(&row["attempts"], &mut counts.attempts)?;
        add_counts(&row["productions"], &mut counts.productions)?;
        if counts
            .productions
            .keys()
            .any(|rule| !counts.configured.contains(rule))
        {
            return Err(Error::Difference(format!(
                "line {}: rule production absent from configured candidates",
                index + 1
            )));
        }
    }
    if steps.is_empty() {
        return Err(Error::Difference("optimizer trace is empty".into()));
    }
    let mut all_configured = BTreeSet::new();
    let mut all_productions = BTreeMap::new();
    let mut rows = Vec::new();
    for ((name, level), counts) in steps {
        all_configured.extend(counts.configured.iter().cloned());
        for (rule, count) in &counts.productions {
            *all_productions.entry(rule.clone()).or_insert(0_u64) += count;
        }
        rows.push(json!({"step": name, "optimization_level": level,
            "invocations": counts.invocations, "incomplete": counts.incomplete,
            "configured_rules": counts.configured, "attempts": counts.attempts,
            "productions": counts.productions, "correlates_before": counts.correlates_before,
            "correlates_after": counts.correlates_after}));
    }
    let unobserved: Vec<_> = all_configured
        .iter()
        .filter(|r| !all_productions.contains_key(*r))
        .collect();
    Ok(
        json!({"schema_version": 1, "configured_rules": all_configured.len(),
        "rules_with_productions": all_productions.len(), "productions": all_productions,
        "configured_without_productions": unobserved, "steps": rows}),
    )
}

#[cfg(test)]
mod tests {
    use super::summarize_records;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn attempts_are_not_productions_and_levels_remain_separate() {
        let mut row = json!({"schema_version": 1, "step": "reduce", "optimization_level": 2,
            "completed": true, "before": {"correlates": [{}]}, "after": {"correlates": []},
            "configured_rules": ["fold", "unused"], "attempts": {"fold": 100}, "productions": {"fold": 2}});
        let first = row.to_string();
        row["optimization_level"] = json!(0);
        row["productions"] = json!({});
        let result = summarize_records(Cursor::new(format!("{first}\n{row}\n"))).unwrap();
        assert_eq!(result["productions"]["fold"], 2);
        assert_eq!(result["configured_without_productions"], json!(["unused"]));
        assert_eq!(result["steps"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn incomplete_and_inconsistent_journals_are_rejected() {
        for input in ["", "{", "{}", "{\"schema_version\":1,\"schema_version\":1}"] {
            assert!(summarize_records(Cursor::new(input)).is_err());
        }
        let row = json!({"schema_version":1,"step":"reduce","optimization_level":2,
            "completed":true,"before":{"correlates":[]},"after":{"correlates":[]},
            "configured_rules":[],"attempts":{"fold":1},"productions":{"fold":1}});
        assert!(summarize_records(Cursor::new(row.to_string())).is_err());
    }
}
