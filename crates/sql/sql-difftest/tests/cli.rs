use serde_json::json;
use std::{fs, process::Command};

#[test]
fn streaming_canonicalization_preserves_boundaries_and_rejects_bad_records() {
    use std::io::Write;
    use std::process::Stdio;
    let dump = json!({
        "schema_version": 1, "producer_commit": "test", "runtime_abi": "test",
        "engine_mode": "classic", "options": {}, "replayable": false,
        "observation": {"stage": "S2", "sequence": 0, "path": []},
        "circuit": {"class": "DBSPCircuit", "id": 1, "allOperators": [], "declarations": []}
    });
    let run = |input: &str| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg("--canonicalize-stream")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    };
    let output = run(&format!("{dump}\n{dump}\n"));
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], lines[1]);
    for invalid in [
        "not JSON",
        "{}",
        r#"{"schema_version":1,"schema_version":1}"#,
    ] {
        let output = run(&format!("{dump}\n{invalid}\n"));
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(String::from_utf8(output.stdout).unwrap().lines().count(), 1);
    }
}

#[test]
fn cli_reports_equality_differences_and_input_errors() {
    let directory = std::env::temp_dir().join(format!("sql-difftest-cli-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let expected = directory.join("expected.json");
    let actual = directory.join("actual.json");
    let dump =
        r#"{"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[],"incremental":true}"#;
    fs::write(&expected, dump).unwrap();
    fs::write(&actual, dump.replace("\"id\":0", "\"id\":100")).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg(&expected)
            .arg(&actual)
            .output()
            .unwrap()
    };
    assert_eq!(run().status.code(), Some(0));
    fs::write(&actual, dump.replace("true", "false")).unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::write(&actual, "not JSON").unwrap();
    assert_eq!(run().status.code(), Some(2));
    fs::remove_file(&actual).unwrap();
    assert_eq!(run().status.code(), Some(2));
    assert_eq!(
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .output()
            .unwrap()
            .status
            .code(),
        Some(2)
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn completed_captures_require_every_observation_in_order() {
    let directory =
        std::env::temp_dir().join(format!("sql-difftest-captures-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let left = directory.join("left");
    let right = directory.join("right");
    fs::create_dir(&left).unwrap();
    fs::create_dir(&right).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .args(["--directories"])
            .arg(&left)
            .arg(&right)
            .output()
            .unwrap()
    };
    // Two empty directories are not evidence of determinism.
    assert_ne!(run().status.code(), Some(0));
    let mut last = json!(null);
    for root in [&left, &right] {
        fs::write(
            root.join("complete.json"),
            json!({
                "schema_version": 1, "dump_count": 3, "final_stage": "S3"
            })
            .to_string(),
        )
        .unwrap();
        for (sequence, stage) in ["S2", "S4", "S3"].into_iter().enumerate() {
            let path = if stage == "S4" {
                json!([{
                    "container": "Optimizer", "pass": "TestPass", "index": 0, "iteration": 0
                }])
            } else {
                json!([])
            };
            last = json!({
                "schema_version": 1, "producer_commit": "test-revision", "runtime_abi": "test-runtime",
                "engine_mode": "classic", "options": {}, "replayable": false,
                "observation": {"stage": stage, "sequence": sequence, "path": path},
                "circuit": {"class": "DBSPCircuit", "id": 1, "declarations": [], "allOperators": []}
            });
            fs::write(
                root.join(format!("{sequence:06}-{stage}.json")),
                last.to_string(),
            )
            .unwrap();
        }
    }
    assert_eq!(run().status.code(), Some(0));
    let final_file = right.join("000002-S3.json");
    let run_envelope = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg("--envelope")
            .arg(left.join("000002-S3.json"))
            .arg(&final_file)
            .output()
            .unwrap()
    };
    assert_eq!(run_envelope().status.code(), Some(0));
    let mut changed = last.clone();
    changed["schema_version"] = json!(2);
    fs::write(&final_file, changed.to_string()).unwrap();
    assert_eq!(run_envelope().status.code(), Some(1));
    assert_eq!(run().status.code(), Some(1));
    // A mismatched sequence fails even when the same corrupt capture is on both sides.
    changed = last.clone();
    changed["observation"]["sequence"] = json!(1);
    for root in [&left, &right] {
        fs::write(root.join("000002-S3.json"), changed.to_string()).unwrap();
    }
    assert_eq!(run().status.code(), Some(1));
    for root in [&left, &right] {
        fs::write(root.join("000002-S3.json"), last.to_string()).unwrap();
    }
    fs::write(right.join("stale.json"), "{}").unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::remove_file(right.join("stale.json")).unwrap();
    fs::remove_file(&final_file).unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::write(&final_file, last.to_string()).unwrap();
    fs::write(
        right.join("complete.json"),
        json!({
            "schema_version": 1, "dump_count": u64::MAX, "final_stage": "S3"
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::remove_file(right.join("complete.json")).unwrap();
    assert_eq!(run().status.code(), Some(2));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn standalone_s3_capture_checks_metadata_and_file_identity() {
    let directory = std::env::temp_dir().join(format!("sql-difftest-s3-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let dump = json!({
        "schema_version": 1, "producer_commit": "test", "runtime_abi": "test",
        "engine_mode": "classic", "options": {}, "replayable": false,
        "observation": {"stage": "S3", "sequence": 0, "path": []},
        "circuit": {"class": "DBSPCircuit", "id": 1, "allOperators": [], "declarations": []}
    });
    fs::write(directory.join("000000-S3.json"), dump.to_string()).unwrap();
    fs::write(
        directory.join("complete.json"),
        json!({
            "schema_version": 1, "dump_count": 1, "final_stage": "S3"
        })
        .to_string(),
    )
    .unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_sql-difftest"))
            .arg("--directories")
            .arg(&directory)
            .arg(&directory)
            .output()
            .unwrap()
    };
    assert!(run().status.success());
    let mut invalid = dump.clone();
    invalid["observation"]["sequence"] = json!(1);
    fs::write(directory.join("000000-S3.json"), invalid.to_string()).unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::write(directory.join("000000-S3.json"), dump.to_string()).unwrap();
    fs::rename(
        directory.join("000000-S3.json"),
        directory.join("000000-S2.json"),
    )
    .unwrap();
    assert_eq!(run().status.code(), Some(1));
    fs::remove_dir_all(directory).unwrap();
}
