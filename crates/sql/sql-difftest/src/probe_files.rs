use crate::{
    diagnostic_comparison::{self, DiagnosticMetrics},
    files::{read, Error},
};
use feldera_sql_difftest::{compare_circuits, parse_json};
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

fn open_manifest(directory: &Path) -> Result<Value, Error> {
    let invalid = |message: &str| Error::Difference(format!("{}: {message}", directory.display()));
    let manifest = read(&directory.join("complete.json"))?;
    if manifest["schema_version"] != 1
        || !manifest["oracle_ready"].is_boolean()
        || manifest["runtime_verified"] != false
        || manifest["compiler_bugs"].as_u64().is_none()
    {
        return Err(invalid("incomplete or invalid probe oracle"));
    }
    for field in ["producer_commit", "calcite_commit", "runtime_commit"] {
        if !manifest[field]
            .as_str()
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(invalid("missing probe build identity"));
        }
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).map_err(|e| Error::Input(e.to_string()))? {
        let entry = entry.map_err(|e| Error::Input(e.to_string()))?;
        if !entry
            .file_type()
            .map_err(|e| Error::Input(e.to_string()))?
            .is_file()
        {
            return Err(invalid("expected regular probe files"));
        }
        files.push(entry.file_name());
    }
    files.sort();
    if files != ["complete.json", "golden.jsonl"] {
        return Err(invalid("missing or extra probe files"));
    }
    Ok(manifest)
}

/// A complete batch may contain compiler bugs, but cannot certify an oracle if it does.
struct ProbeBatch {
    manifest: Value,
    reader: BufReader<File>,
    total: u64,
    accepted: u64,
    bugs: u64,
}

impl ProbeBatch {
    fn open(directory: &Path) -> Result<Self, Error> {
        let manifest = open_manifest(directory)?;
        let input = File::open(directory.join("golden.jsonl"))
            .map_err(|e| Error::Input(format!("{}: {e}", directory.display())))?;
        Ok(Self {
            manifest,
            reader: BufReader::new(input),
            total: 0,
            accepted: 0,
            bugs: 0,
        })
    }

    fn next(&mut self) -> Result<Option<Value>, Error> {
        let mut line = String::new();
        if self
            .reader
            .read_line(&mut line)
            .map_err(|e| Error::Input(e.to_string()))?
            == 0
        {
            self.check_counts()?;
            return Ok(None);
        }
        let invalid = |message: &str| Error::Difference(format!("probe {}: {message}", self.total));
        let row = parse_json(line.as_bytes()).map_err(|e| invalid(&e.to_string()))?;
        if row["schema_version"] != 1
            || !row["compiler_bug"].is_boolean()
            || row["probe"]["schema_version"] != 1
            || row["probe"]["id"].as_u64() != Some(self.total)
            || !row["probe"]["program"].is_string()
            || !row["schema"].is_object()
            || !row["diagnostics"].is_array()
            || row.pointer("/options/ioOptions/inputCircuit") != Some(&json!(true))
            || row.pointer("/options/languageOptions/optimizationLevel") != Some(&json!(0))
        {
            return Err(invalid("invalid record identity or options"));
        }
        let mut has_error = false;
        let mut has_bug = false;
        for diagnostic in row["diagnostics"].as_array().unwrap() {
            let (Some(warning), Some(kind), Some(_)) = (
                diagnostic["warning"].as_bool(),
                diagnostic["error_type"].as_str(),
                diagnostic["message"].as_str(),
            ) else {
                return Err(invalid("malformed diagnostic"));
            };
            has_error |= !warning;
            has_bug |=
                kind == "Compiler error" || kind.starts_with("This is a bug in the compiler");
        }
        if row["compiler_bug"].as_bool() != Some(has_bug)
            || row["accepted"].as_bool() != Some(!has_error)
        {
            return Err(invalid(
                "verdict or compiler-bug flag contradicts diagnostics",
            ));
        }
        match row["accepted"].as_bool() {
            Some(true) if row["compiler_bug"] == false => {
                compare_circuits(&row["s2"], &row["s2"]).map_err(|e| invalid(&e.to_string()))?;
                self.accepted += 1;
            }
            Some(false)
                if row.get("s2").is_none()
                    && row["diagnostics"].as_array().is_some_and(|a| !a.is_empty()) => {}
            _ => return Err(invalid("invalid acceptance or rejection")),
        }
        self.bugs += u64::from(row["compiler_bug"] == true);
        self.total += 1;
        Ok(Some(row))
    }

    fn check_counts(&self) -> Result<(), Error> {
        if self.total == 0
            || self.manifest["expected_probes"].as_u64() != Some(self.total)
            || self.manifest["probes"].as_u64() != Some(self.total)
            || self.manifest["accepted"].as_u64() != Some(self.accepted)
            || self.manifest["rejected"].as_u64() != Some(self.total - self.accepted)
            || self.manifest["compiler_bugs"].as_u64() != Some(self.bugs)
            || self.manifest["oracle_ready"].as_bool() != Some(self.bugs == 0)
        {
            return Err(Error::Difference(
                "probe counts disagree with completion manifest".into(),
            ));
        }
        Ok(())
    }
}

/// Check the complete Java probe batch, keeping only one result in memory.
pub fn validate(directory: &Path) -> Result<Value, Error> {
    let mut batch = ProbeBatch::open(directory)?;
    if batch.manifest["oracle_ready"] != true {
        return Err(Error::Difference(
            "compiler bugs are not valid rejection goldens".into(),
        ));
    }
    while batch.next()?.is_some() {}
    Ok(batch.manifest)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiagnosticPolicy {
    Exact,
    C0,
}

/// Report changes in verdicts, schemas, diagnostics, and canonical S2 graphs.
pub fn compare(expected: &Path, actual: &Path, policy: DiagnosticPolicy) -> Result<Value, Error> {
    let mut expected = ProbeBatch::open(expected)?;
    let mut actual = ProbeBatch::open(actual)?;
    if policy == DiagnosticPolicy::C0
        && (expected.manifest["oracle_ready"] != true || actual.manifest["oracle_ready"] != true)
    {
        return Err(Error::Difference(
            "C0 probe comparison requires batches without compiler failures".into(),
        ));
    }
    let mut diagnostic_metrics = DiagnosticMetrics::default();
    let mut changes = Vec::new();
    loop {
        let pair = (expected.next()?, actual.next()?);
        let (left, right) = match pair {
            (None, None) => break,
            (Some(left), Some(right)) => (left, right),
            _ => return Err(Error::Difference("probe batch lengths differ".into())),
        };
        if left["probe"] != right["probe"] || left["options"] != right["options"] {
            return Err(Error::Difference(format!(
                "probe {}: inputs or options differ",
                left["probe"]["id"]
            )));
        }
        let mut fields = Vec::new();
        for field in ["accepted", "compiler_bug", "schema"] {
            if left[field] != right[field] {
                fields.push(field);
            }
        }
        let diagnostics_equal = match policy {
            DiagnosticPolicy::Exact => left["diagnostics"] == right["diagnostics"],
            DiagnosticPolicy::C0 => diagnostic_comparison::compare(
                &left["probe"]["id"],
                &left["diagnostics"],
                &right["diagnostics"],
                &mut diagnostic_metrics,
            )?,
        };
        if !diagnostics_equal {
            fields.push("diagnostics");
        }
        match (left.get("s2"), right.get("s2")) {
            (Some(left), Some(right)) if compare_circuits(left, right).is_err() => {
                fields.push("s2")
            }
            (Some(_), None) | (None, Some(_)) => fields.push("s2"),
            _ => {}
        }
        if !fields.is_empty() {
            changes.push(json!({
                "probe_id": left["probe"]["id"], "operation": left["probe"]["operation"],
                "fields": fields,
                "before": {"accepted": left["accepted"], "compiler_bug": left["compiler_bug"]},
                "after": {"accepted": right["accepted"], "compiler_bug": right["compiler_bug"]},
            }));
        }
    }
    let mut report = json!({
        "schema_version": 1, "equal": changes.is_empty(), "compared": expected.total,
        "unchanged": expected.total - changes.len() as u64,
        "expected": expected.manifest, "actual": actual.manifest, "changes": changes,
    });
    if policy == DiagnosticPolicy::C0 {
        report["c0_diagnostic_metrics"] = json!(diagnostic_metrics);
    }
    Ok(report)
}
