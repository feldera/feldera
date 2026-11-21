use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "type-probe-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn run(&self, input: &str) -> Output {
        fs::write(self.0.join("input.jsonl"), input).unwrap();
        Command::new(env!("CARGO_BIN_EXE_feldera-type-derivation-probe"))
            .arg(self.0.join("input.jsonl"))
            .arg(self.0.join("output.jsonl"))
            .output()
            .unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn probe() -> Value {
    json!({"schema_version":1,"id":0,"operation":"ABS","program":"deliberately not SQL",
        "arguments":[{"type":"DECIMAL(10,2)","nullable":true,"origin":"column","boundary_value":"unused"}]})
}

#[test]
fn declared_types_are_the_only_inference_input() {
    let mut records = Vec::new();
    for (id, origin, sql) in [
        (0, "literal", "not SQL"),
        (1, "column", "CREATE VIEW x AS SELECT 999"),
    ] {
        let mut input = probe();
        input["id"] = json!(id);
        input["arguments"][0]["origin"] = json!(origin);
        input["program"] = json!(sql);
        records.push(input.to_string());
    }
    let scratch = Scratch::new();
    let output = scratch.run(&records.join("\n"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let outcomes: Vec<Value> = fs::read_to_string(scratch.0.join("output.jsonl"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(outcomes[0]["translated"], outcomes[1]["translated"]);
    assert_eq!(outcomes[0]["rewritten"], outcomes[1]["rewritten"]);
    assert_eq!(outcomes[0]["runtime_verified"], false);
}

#[test]
fn duplicate_ids_and_unknown_fields_are_errors() {
    let input = probe().to_string();
    let scratch = Scratch::new();
    assert!(!scratch.run(&format!("{input}\n{input}\n")).status.success());
    let scratch = Scratch::new();
    let mut unknown = probe();
    unknown["oracle_answer"] = json!("never consume oracle data");
    assert!(!scratch.run(&unknown.to_string()).status.success());
}

#[test]
fn duplicate_json_keys_and_oversize_records_are_errors() {
    let scratch = Scratch::new();
    let input = probe()
        .to_string()
        .replacen("\"id\":0", "\"id\":0,\"id\":1", 1);
    assert!(!scratch.run(&input).status.success());
    let scratch = Scratch::new();
    assert!(!scratch.run(&" ".repeat(65_537)).status.success());
}

#[test]
fn output_is_exclusive_and_invalid_types_fail() {
    let scratch = Scratch::new();
    fs::write(scratch.0.join("output.jsonl"), "keep").unwrap();
    assert!(!scratch.run(&probe().to_string()).status.success());
    assert_eq!(
        fs::read_to_string(scratch.0.join("output.jsonl")).unwrap(),
        "keep"
    );
    let scratch = Scratch::new();
    let mut input = probe();
    input["arguments"][0]["type"] = json!("DECIMAL(38,39)");
    assert!(!scratch.run(&input.to_string()).status.success());
}
