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
fn advisory_preview_distinguishes_sql_errors_from_unsupported_programs() {
    let preview = |sql| {
        let output = invoke("--preview", sql);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["advisory"], true);
        result
    };
    let valid = preview("CREATE MATERIALIZED VIEW v AS SELECT 3 AS x;");
    assert_eq!(valid["status"], "bound");
    assert_eq!(valid["schema"]["outputs"][1]["materialized"], true);
    let decimals = preview("CREATE TABLE t(\n  n NUMERIC, m NUMBER, d DECIMAL(8,2));");
    assert_eq!(decimals["status"], "bound");
    let warnings = decimals["messages"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["warning"], true);
    assert_eq!(
        warnings[0]["error_type"],
        "DECIMAL precision and scale unspecified"
    );
    assert_eq!(warnings[0]["start_line_number"], 2);
    assert_eq!(warnings[0]["start_column"], 5);
    assert!(warnings[0]["snippet"].as_str().unwrap().contains("^^^^^^^"));
    for (sql, class) in [
        ("CREATE TABLE t(x INT(10))", "Error parsing SQL"),
        ("CREATE TABLE sqrt(x INT);", "Reserved name"),
    ] {
        let error = preview(sql);
        assert_eq!(error["status"], "error");
        assert_eq!(error["messages"][0]["error_type"], class);
        assert!(error["messages"][0]["snippet"]
            .as_str()
            .unwrap()
            .contains('^'));
    }
    let unsupported = preview("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.x = b.x;");
    assert_eq!(unsupported["status"], "unsupported");
    assert!(unsupported["reason"].is_string());
    assert_eq!(unsupported["messages"], serde_json::json!([]));
}

#[test]
fn preview_preserves_public_column_names_and_primary_key_order() {
    let sql = "CREATE TABLE t(a INT NOT NULL, PRIMARY KEY (b, a), b INT NOT NULL);
               CREATE LOCAL VIEW swapped(left_col, right_col) AS SELECT b, a FROM t;
               CREATE VIEW result AS SELECT right_col FROM swapped;";
    let output = invoke("--preview", sql);
    let preview: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(preview["status"], "bound");
    let table = &preview["schema"]["inputs"][0];
    assert_eq!(table["primary_key"], serde_json::json!(["a", "b"]));
    let public = &preview["schema"]["outputs"][1];
    assert_eq!(public["fields"][0]["name"], "right_col");
    for invalid in [
        "CREATE TABLE t(a INT, PRIMARY KEY (a));",
        "CREATE TABLE t(a INT NOT NULL, PRIMARY KEY (missing));",
        "CREATE TABLE t(a INT NOT NULL PRIMARY KEY, PRIMARY KEY (a));",
        "CREATE TABLE t(a INT NOT NULL, PRIMARY KEY (a, a));",
        "CREATE VIEW v(a, b) AS SELECT 1;",
        "CREATE TABLE t(x INT, y INT); CREATE VIEW v(a, a) AS SELECT x, y FROM t;",
    ] {
        assert!(!invoke("--schema", invalid).status.success(), "{invalid}");
    }
}

#[test]
fn supplementary_literals_share_the_operator_but_keep_their_positions() {
    use serde_json::Value;
    let sql = "CREATE VIEW v AS SELECT 'é' AS a, '😀' AS b, ('🐕') AS c;";
    let output = invoke("--emit-s1", sql);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let bound: Value = serde_json::from_slice(&output.stdout).unwrap();
    let graph = &bound["expressions"];
    let objects = graph["operator_objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["key"], "non-bmp-literal");
    let calls: Vec<_> = graph["expressions"].as_array().unwrap().iter()
        .filter(|e| e["kind"]["kind"] == "call").collect();
    assert_eq!(calls.len(), 2);
    for (call, spelling) in calls.iter().zip(["'😀'", "('🐕')"]) {
        let offset = sql[..sql.find(spelling).unwrap()].encode_utf16().count();
        let position = &call["position"]["position"];
        assert_eq!(position["column"], offset + 1);
        assert_eq!(position["end_column"], offset + spelling.encode_utf16().count());
    }
}

#[test]
fn insert_values_keep_submission_ranges_and_destination_order() {
    use serde_json::{json, Value};
    let request = json!({"id":"insert", "submissions":[
        {"sql":"CREATE TABLE t(a INT, b BOOLEAN NOT NULL)", "many":false, "batch":0},
        {"sql":"-- load rows\nINSERT INTO t(B,A) VALUES (TRUE, NULL), (FALSE, 9)", "many":false, "batch":0},
        {"sql":"CREATE VIEW v AS SELECT * FROM t", "many":false, "batch":0}
    ]});
    let result = invoke("--bind-submissions", &request.to_string());
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    let bound: Value = serde_json::from_slice(&result.stdout).unwrap();
    let insert = &bound["statements"][4];
    assert_eq!(insert["source"]["position"]["line"], 3);
    assert_eq!(insert["columns"][0]["name"]["name"], "b");
    assert_eq!(insert["columns"][1]["position"]["column"], 17);
    let id = insert["relation"].as_u64().unwrap() as usize;
    assert_eq!(bound["properties"]["relations"][id]["fields"]["tuples"], 2);
    let roots = &bound["expressions"]["relation_uses"][id]["roots"];
    let literals: Vec<_> = roots.as_array().unwrap().iter().map(|id| {
        &bound["expressions"]["expressions"][id.as_u64().unwrap() as usize]["kind"]["literal"]["value"]
    }).collect();
    assert_eq!(json!(literals), json!([
        {"kind":"boolean","value":true}, {"kind":"null"}, {"kind":"boolean","value":false},
        {"kind":"integer","value":"9"}
    ]));
    for invalid in [
        "INSERT INTO missing VALUES (1, TRUE)",
        "INSERT INTO t(a,a) VALUES (1, 2)",
        "INSERT INTO t VALUES (1, NULL)",
        "INSERT INTO t VALUES (1, TRUE), (4)",
        "INSERT INTO t VALUES (1, TRUE) LIMIT 0",
    ] {
        let sql = format!("CREATE TABLE t(a INT, b BOOLEAN NOT NULL); {invalid}");
        assert!(!invoke("--schema", &sql).status.success(), "{sql}");
    }
    for assignment in [
        "INSERT INTO t VALUES (2147483648, FALSE)",
        "INSERT INTO t(b,a) VALUES (7, NULL)",
    ] {
        let sql = format!("CREATE TABLE t(a INT, b BOOLEAN NOT NULL); {assignment}");
        assert!(invoke("--schema", &sql).status.success(), "{sql}");
    }
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
fn deep_checkpoint_artifacts_preserve_definitions_and_exact_literals() {
    // Node headers can arrive after their children. The last literal's type
    // refers back to a definition outside the nested subtree.
    let literal = r#"{"id":2,"value":12345678901234567890.000001,"type":{"node":1},"class":"DBSPDecimalLiteral"}"#;
    let mut expression = literal.to_owned();
    for id in 3..173 {
        expression =
            format!(r#"{{"source":{expression},"class":"DBSPCloneExpression","id":{id}}}"#);
    }
    let graph = format!(
        r#"{{"class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[],"metadata":{{"inputs":[],"outputs":[]}},"ty":{{"class":"DBSPTypeDecimal","id":1}},"expression":{expression}}}"#
    );
    let checkpoint = |graph: &str, gen2: bool| {
        format!(
            r#"{{"schema_version":4,"stage":"S3-subset","options":{{"ioOptions":{{"gen2":{gen2}}}}},"sources":{{"lines":[]}},"calcite_plan":{{}},"diagnostics":{{"messages":[]}},"circuit":{graph}}}"#
        )
    };
    for gen2 in [false, true] {
        let result = invoke("--s3-to-artifacts", &checkpoint(&graph, gen2));
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let artifacts = feldera_sql_ir::parse_circuit_json(&result.stdout).unwrap();
        let circuit = if gen2 {
            &artifacts["circuit"]
        } else {
            &artifacts["checkpoint"]["circuit"]
        };
        let mut node = &circuit["expression"];
        for id in (3..173).rev() {
            assert_eq!(node["id"], id);
            node = &node["source"];
        }
        assert_eq!(node["value"].to_string(), "12345678901234567890.000001");
        assert_eq!(node["type"]["node"], 1);
    }
    let mut plan = serde_json::json!({"input": "column"});
    for _ in 0..150 {
        plan = serde_json::json!({"args": [plan]});
    }
    let deep_plan = checkpoint(&graph, false)
        .replace(r#""calcite_plan":{}"#, &format!(r#""calcite_plan":{plan}"#));
    let result = invoke("--s3-to-artifacts", &deep_plan);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let artifacts = feldera_sql_ir::parse_circuit_json(&result.stdout).unwrap();
    assert_eq!(artifacts["checkpoint"]["calcite_plan"], plan);
    assert_eq!(artifacts["dataflow"]["calcite_plan"], plan);
    let duplicate = graph.replace(r#""value":"#, r#""value":0,"value":"#);
    for mode in ["--s3-to-artifacts", "--s3-to-diagnostics"] {
        let result = invoke(mode, &checkpoint(&duplicate, true));
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("duplicate"));
    }
    let missing_type = graph.replace(r#"{"node":1}"#, r#"{"node":999}"#);
    let result = invoke("--s3-to-artifacts", &checkpoint(&missing_type, false));
    assert!(String::from_utf8_lossy(&result.stderr).contains("undefined node"));
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

#[test]
fn worker_publishes_resource_limits_without_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = dir.path().join("input.json");
    let attempt = dir.path().join("attempt");
    let diagnostic = serde_json::json!({
        "range": feldera_sql_source::SourceRange::default(),
        "type": "Optimizer resource limit", "message": "Optimizer input exceeds the limit", "warning": false
    });
    let input = serde_json::json!({
        "schema_version":4, "stage":"S3-subset", "producer_commit":"test", "runtime_abi":"test",
        "options":{"ioOptions":{"gen2":true}}, "sources":{"lines":[]}, "calcite_plan":{},
        "diagnostics":{"messages":[diagnostic]},
        "circuit":{"class":"DBSPCircuit","id":0,"metadata":{"inputs":[],"outputs":[]},
                   "declarations":[],"allOperators":[]}
    });
    std::fs::write(&checkpoint, input.to_string()).unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_sql2dbsp"))
        .args(["--s3-worker", "test", "test"])
        .arg(checkpoint)
        .arg(&attempt)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(1));
    let result = attempt.join("result");
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(result.join("result.json")).unwrap()).unwrap();
    assert_eq!(record["status"], "resource_limit", "{record}");
    assert!(record["reason"]
        .as_str()
        .unwrap()
        .contains("Optimizer input exceeds the limit"));
    assert_eq!(std::fs::read_dir(result).unwrap().count(), 1);
}
