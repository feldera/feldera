use crate::files::{read, read_with, Error};
use feldera_sql_difftest::corpus::{portable_output_zset, validate_case};
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs, path::Path};

fn invalid(path: &Path, message: &str) -> Error {
    Error::Difference(format!("{}: {message}", path.display()))
}

/// Validate every per-test manifest and case, retaining one case at a time.
/// The caller must also check Maven's status: missing whole tests cannot be
/// detected from the export alone.
pub fn validate(directory: &Path, portable_outputs: bool) -> Result<Value, Error> {
    let mut tests = BTreeSet::new();
    let mut revisions = None;
    let (mut exported, mut skipped, mut steps, mut errors, mut compile_only) = (0, 0, 0, 0, 0);
    let (mut portable_bags, mut portable_rows) = (0, 0);
    let entries = fs::read_dir(directory).map_err(|e| Error::Input(e.to_string()))?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error::Input(e.to_string()))?;
        if !entry
            .file_type()
            .map_err(|e| Error::Input(e.to_string()))?
            .is_dir()
        {
            return Err(invalid(&entry.path(), "expected a test directory"));
        }
        paths.push(entry.path());
    }
    paths.sort();
    for path in paths {
        let manifest = read(&path.join("complete.json"))?;
        let count = manifest["case_count"]
            .as_u64()
            .ok_or_else(|| invalid(&path, "missing case count"))?;
        let source = manifest["source_test"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid(&path, "missing test identity"))?;
        if manifest["schema_version"] != 1 || !tests.insert(source.to_owned()) {
            return Err(invalid(
                &path,
                "unknown schema version or duplicate test identity",
            ));
        }
        match manifest["test_body_status"].as_str() {
            Some("passed") => {}
            Some("skipped" | "failed")
                if manifest["reason"]
                    .as_str()
                    .is_some_and(|s| !s.trim().is_empty()) => {}
            _ => return Err(invalid(&path, "invalid test status or missing reason")),
        }
        let mut names = BTreeSet::new();
        for entry in fs::read_dir(&path).map_err(|e| Error::Input(e.to_string()))? {
            let entry = entry.map_err(|e| Error::Input(e.to_string()))?;
            if !entry
                .file_type()
                .map_err(|e| Error::Input(e.to_string()))?
                .is_file()
            {
                return Err(invalid(&entry.path(), "expected a regular file"));
            }
            names.insert(entry.file_name());
        }
        if names.len() as u64 != count.saturating_add(1) {
            return Err(invalid(&path, "missing or extra corpus files"));
        }
        let (mut test_exported, mut test_skipped) = (0_u64, 0_u64);
        for index in 0..count {
            let case_path = path.join(format!("case-{index:04}.json"));
            let case = read_with(&case_path, feldera_sql_ir::parse_circuit_envelope)?;
            let summary = validate_case(&case).map_err(|e| invalid(&case_path, &e.to_string()))?;
            if case["source_test"] != source || case["case_index"].as_u64() != Some(index) {
                return Err(invalid(
                    &case_path,
                    "case identity disagrees with manifest or filename",
                ));
            }
            let identity = [
                case["producer_commit"].clone(),
                case["calcite_commit"].clone(),
                case["runtime_commit"].clone(),
            ];
            if revisions
                .as_ref()
                .is_some_and(|previous| previous != &identity)
            {
                return Err(invalid(&case_path, "mixed build identities in one corpus"));
            }
            revisions = Some(identity);
            if summary.exported {
                test_exported += 1;
                steps += summary.steps;
                errors += usize::from(summary.expected_error);
                compile_only += usize::from(summary.steps == 0 && !summary.expected_error);
                if portable_outputs {
                    for (command, body) in case["commands"].as_array().unwrap().iter().enumerate() {
                        if let Some(outputs) = body["outputs"].as_array() {
                            for (index, table) in outputs.iter().enumerate() {
                                let bag =
                                    portable_output_zset(&table["zset_ir"]).map_err(|error| {
                                        invalid(
                                            &case_path,
                                            &format!(
                                                "/commands/{command}/outputs/{index}/zset_ir{error}"
                                            ),
                                        )
                                    })?;
                                portable_bags += 1;
                                portable_rows += bag.row_count();
                            }
                        }
                    }
                }
            } else {
                test_skipped += 1;
            }
        }
        if manifest["exported"].as_u64() != Some(test_exported)
            || manifest["skipped"].as_u64() != Some(test_skipped)
            || (manifest["test_body_status"] == "passed" && test_skipped != 0)
            || (manifest["test_body_status"] != "passed" && test_exported != 0)
        {
            return Err(invalid(&path, "case status counts disagree with manifest"));
        }
        exported += test_exported;
        skipped += test_skipped;
    }
    if exported == 0 {
        return Err(invalid(directory, "corpus contains no exported cases"));
    }
    let mut summary = json!({"schema_version": 1, "tests": tests.len(), "exported": exported,
        "skipped": skipped, "steps": steps, "expected_errors": errors,
        "compile_only": compile_only, "build_identity": revisions,
        "runtime_verified": false});
    if portable_outputs {
        summary["portable_bags"] = json!(portable_bags);
        summary["portable_rows"] = json!(portable_rows);
    }
    Ok(summary)
}
