use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn invoke(mode: &str, source: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sql2dbsp"))
        .args([mode, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(source.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn checks_full_submissions_and_reports_the_original_error_line() {
    let valid = invoke("--check-syntax", "CREATE TABLE readings (sensor INT, temp INT);\nCREATE VIEW hot AS SELECT sensor FROM readings WHERE temp > 20;");
    assert!(
        valid.status.success(),
        "{}",
        String::from_utf8_lossy(&valid.stderr)
    );
    let invalid = invoke(
        "--check-syntax",
        "CREATE TABLE readings (sensor INT);\nSELECT (",
    );
    assert_eq!(invalid.status.code(), Some(1));
    let error = String::from_utf8(invalid.stderr).unwrap();
    assert!(
        error.contains("<stdin>") && error.contains("Line: 2"),
        "{error}"
    );
}

#[test]
fn refuses_the_wrong_stage_version_and_engine_before_emission() {
    let checkpoint = serde_json::json!({
        "schema_version":4, "stage":"S3-subset", "options":{"ioOptions":{"gen2":true}},
        "sources":{"lines":[]}, "calcite_plan":{}, "diagnostics":{"messages":[]},
        "circuit":{"class":"DBSPCircuit","id":0,"metadata":{"inputs":[],"outputs":[]},
                   "declarations":[],"allOperators":[]}
    });
    let run = |checkpoint: &serde_json::Value| invoke("--s3-to-artifacts", &checkpoint.to_string());
    let result = run(&checkpoint);
    assert!(result.status.success());
    let output: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(output["diagnostics"], serde_json::json!([]));
    assert_eq!(output["schema"], checkpoint["circuit"]["metadata"]);
    assert_eq!(output["dataflow"]["mir"], serde_json::json!({}));
    let mut classic = checkpoint.clone();
    classic["options"]["ioOptions"]["gen2"] = false.into();
    let result = run(&classic);
    assert!(result.status.success());
    let artifacts: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(artifacts["checkpoint"], classic);
    assert!(artifacts.get("circuit").is_none());
    assert!(!invoke("--s3-to-gen2", &classic.to_string())
        .status
        .success());
    for (path, value) in [
        ("/stage", "S2-subset".into()),
        ("/schema_version", 3.into()),
        ("/options/ioOptions/gen2", "false".into()),
        ("/sources/lines", 1.into()),
    ] {
        let mut invalid = checkpoint.clone();
        *invalid.pointer_mut(path).unwrap() = value;
        let output = run(&invalid);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn binding_submissions_preserves_the_api_source_and_batch_boundary() {
    use serde_json::{json, Value};

    let mut request = json!({"id":"separate-calls", "submissions":[
        {"sql":"CREATE TABLE readings (v INT)\n", "many":false, "batch":0},
        {"sql":"CREATE VIEW result AS SELECT v FROM readings; CREATE VIEW fixed AS SELECT 5 AS x",
         "many":true, "batch":0}
    ]});
    let run = |request: &Value| invoke("--bind-submissions", &request.to_string());
    let output = run(&request);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bound: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(bound["id"], "separate-calls");
    assert_eq!(bound["context"]["sources"]["program"],
        "CREATE TABLE readings (v INT)\n\nCREATE VIEW result AS SELECT v FROM readings; CREATE VIEW fixed AS SELECT 5 AS x\n");
    assert_eq!(bound["statements"][4]["source"]["position"]["line"], 2);
    request["submissions"][1]["batch"] = 1.into();
    let output = run(&request);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("one compilation batch"));
}
