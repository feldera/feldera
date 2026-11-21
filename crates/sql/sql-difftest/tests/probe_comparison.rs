use serde_json::{json, Value};
use std::{fs, path::Path, process::Command};

fn row() -> Value {
    json!({"schema_version": 1, "compiler_bug": false, "accepted": true,
    "probe": {"schema_version": 1, "id": 0, "program": "CREATE VIEW v AS SELECT 1", "operation": "constant"},
    "schema": {}, "diagnostics": [],
    "options": {"ioOptions": {"inputCircuit": true}, "languageOptions": {"optimizationLevel": 0}},
    "s2": {"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
        {"class": "DBSPConstantOperator", "id": 1, "value": 1}
    ]}})
}

fn write_batch(directory: &Path, row: &Value) {
    fs::create_dir_all(directory).unwrap();
    let bugs = u64::from(row["compiler_bug"] == true);
    let accepted = u64::from(row["accepted"] == true);
    let manifest = json!({"schema_version": 1, "oracle_ready": bugs == 0,
        "runtime_verified": false, "compiler_bugs": bugs, "probes": 1,
        "accepted": accepted, "rejected": 1 - accepted, "expected_probes": 1,
        "producer_commit": "compiler", "calcite_commit": "calcite", "runtime_commit": "runtime"});
    fs::write(directory.join("complete.json"), manifest.to_string()).unwrap();
    fs::write(directory.join("golden.jsonl"), row.to_string()).unwrap();
}

fn compare(left: &Path, right: &Path) -> std::process::Output {
    compare_with("--compare-probes", left, right)
}

fn compare_with(mode: &str, left: &Path, right: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
        .arg(mode)
        .arg(left)
        .arg(right)
        .output()
        .unwrap()
}

#[test]
fn c0_probes_require_structured_parity_and_report_reviewable_changes() {
    let root = std::env::temp_dir().join(format!("probe-comparison-c0-{}", std::process::id()));
    let (left, right) = (root.join("left"), root.join("right"));
    let mut expected = row();
    let diagnostic = json!({
        "warning": true, "error_type": "Unused view", "message": "The view is unused",
        "start_line_number": 1, "start_column": 13, "end_line_number": 1, "end_column": 14,
        "snippet": "CREATE VIEW v AS SELECT 1"
    });
    expected["diagnostics"] = json!([diagnostic, diagnostic]);
    write_batch(&left, &expected);

    let mut actual = expected.clone();
    actual["diagnostics"][0]["message"] = json!("Nothing reads this view");
    actual["diagnostics"][1]["end_column"] = json!(15);
    write_batch(&right, &actual);
    assert_eq!(compare(&left, &right).status.code(), Some(1));
    let structured = compare_with("--compare-probes-c0", &left, &right);
    assert!(structured.status.success(), "{:?}", structured);
    let report: Value = serde_json::from_slice(&structured.stdout).unwrap();
    assert_eq!(
        report["c0_diagnostic_metrics"],
        json!({"matched_diagnostics": 2, "messages_changed": 1, "end_positions_changed": 1})
    );

    for (field, value) in [
        ("error_type", json!("Other warning")),
        ("start_line_number", json!(2)),
        ("start_column", json!(14)),
        ("snippet", json!("Other SQL")),
    ] {
        let mut changed = actual.clone();
        changed["diagnostics"][0][field] = value;
        write_batch(&right, &changed);
        let result = compare_with("--compare-probes-c0", &left, &right);
        assert_eq!(result.status.code(), Some(1));
        let report: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["changes"][0]["fields"], json!(["diagnostics"]));
    }
    let mut rejected = actual.clone();
    rejected["accepted"] = json!(false);
    rejected["diagnostics"][0]["warning"] = json!(false);
    rejected.as_object_mut().unwrap().remove("s2");
    write_batch(&right, &rejected);
    let result = compare_with("--compare-probes-c0", &left, &right);
    assert_eq!(result.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        report["changes"][0]["fields"],
        json!(["accepted", "diagnostics", "s2"])
    );
    actual["diagnostics"].as_array_mut().unwrap().pop();
    write_batch(&right, &actual);
    assert_eq!(
        compare_with("--compare-probes-c0", &left, &right)
            .status
            .code(),
        Some(1)
    );
    actual["diagnostics"][0]["end_column"] = json!("14");
    write_batch(&right, &actual);
    let malformed = compare_with("--compare-probes-c0", &left, &right);
    assert_eq!(malformed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("invalid actual diagnostics"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compares_canonical_graphs_and_every_observable_field() {
    let root = std::env::temp_dir().join(format!("probe-comparison-{}", std::process::id()));
    let (left, right) = (root.join("left"), root.join("right"));
    let mut changed = row();
    write_batch(&left, &changed);
    changed["s2"]["id"] = json!(100);
    changed["s2"]["allOperators"][0]["id"] = json!(200);
    write_batch(&right, &changed);
    let result = compare(&left, &right);
    assert_eq!(result.status.code(), Some(0), "{:?}", result);
    changed["s2"]["allOperators"][0]["value"] = json!(2);
    changed["schema"] = json!({"outputs": []});
    changed["diagnostics"] =
        json!([{"message": "warning", "warning": true, "error_type": "Warning"}]);
    write_batch(&right, &changed);
    let result = compare(&left, &right);
    assert_eq!(result.status.code(), Some(1));
    let summary: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        summary["changes"][0]["fields"],
        json!(["schema", "diagnostics", "s2"])
    );
    assert_eq!(summary["unchanged"], 0);
    changed["probe"]["program"] = json!("different input");
    write_batch(&right, &changed);
    assert_eq!(compare(&left, &right).status.code(), Some(1));
    // Two equally truncated files must not compare successfully.
    fs::write(left.join("golden.jsonl"), "").unwrap();
    fs::write(right.join("golden.jsonl"), "").unwrap();
    assert_eq!(compare(&left, &right).status.code(), Some(1));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn reports_bug_fixes_without_certifying_a_broken_oracle() {
    let root = std::env::temp_dir().join(format!("probe-bug-fix-{}", std::process::id()));
    let (left, right) = (root.join("left"), root.join("right"));
    let mut broken = row();
    broken["accepted"] = json!(false);
    broken["compiler_bug"] = json!(true);
    broken["diagnostics"] = json!([{"message": "AssertionError", "warning": false,
        "error_type": "This is a bug in the compiler (please report it to the developers)"}]);
    broken.as_object_mut().unwrap().remove("s2");
    write_batch(&left, &broken);
    write_batch(&right, &row());
    let c0 = compare_with("--compare-probes-c0", &left, &right);
    assert_eq!(c0.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&c0.stderr).contains("requires batches without compiler failures")
    );
    let result = compare(&left, &right);
    assert_eq!(result.status.code(), Some(1));
    let summary: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(summary["expected"]["compiler_bugs"], 1);
    assert_eq!(summary["changes"][0]["after"]["accepted"], true);
    assert_eq!(summary["changes"].as_array().unwrap().len(), 1);
    let validation = Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
        .arg("--validate-probes")
        .arg(&left)
        .output()
        .unwrap();
    assert_eq!(validation.status.code(), Some(1));
    // Neither a lying verdict nor a hidden compiler bug can certify a golden.
    for field in ["accepted", "compiler_bug"] {
        let mut mislabeled = broken.clone();
        mislabeled[field] = json!(field == "accepted");
        write_batch(&left, &mislabeled);
        let result = compare(&left, &right);
        assert_eq!(result.status.code(), Some(1));
        assert!(
            result.stdout.is_empty(),
            "malformed input produced a comparison report"
        );
    }
    write_batch(&left, &broken);
    // A truthful manifest is required even when diagnosing a failing batch.
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(left.join("complete.json")).unwrap()).unwrap();
    manifest["compiler_bugs"] = json!(2);
    fs::write(left.join("complete.json"), manifest.to_string()).unwrap();
    assert_eq!(compare(&left, &right).status.code(), Some(1));
    fs::remove_dir_all(root).unwrap();
}
