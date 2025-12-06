use colored::Colorize;
use pipeline_manager::logging::init_logging;
use serde_json::Value;
use std::process::Command;
use tracing::info;

#[test]
fn emits_sample_log() {
    // When invoked as a subprocess, just emit the logs and exit so the parent can assert on output.
    if let Ok(mode) = std::env::var("LOGGING_DEMO_CHILD") {
        emit_logs(mode == "json");
        return;
    }

    let text_out = run_child("text");
    let text_lines: Vec<_> = text_out.lines().collect();
    let prefix_line = text_lines
        .iter()
        .find(|line| line.contains("logging demo event"))
        .expect("expected text log line with demo event");
    assert!(
        prefix_line.starts_with("[logging-demo] "),
        "text log should include the pipeline prefix: {text_out:?}"
    );
    assert!(
        prefix_line.contains("logging_demo: logging demo event"),
        "text log should include the message: {text_out:?}"
    );
    let typed_line = text_lines
        .iter()
        .find(|line| line.contains("typed field coverage"))
        .expect("expected typed field text log line");
    assert!(
        typed_line.contains("demo_int=42"),
        "text log should include typed field output: {text_out:?}"
    );
    assert!(
        typed_line.contains("demo_u64=18446744073709551615"),
        "text log should include u64 field output: {text_out:?}"
    );

    let json_out = run_child("json");
    let json_lines: Vec<_> = json_out.lines().collect();
    let json_line = json_lines
        .iter()
        .rev()
        .find(|line| line.trim_start().starts_with('{'))
        .expect("expected JSON log lines from subprocess");
    let parsed: Value = serde_json::from_str(json_line)
        .unwrap_or_else(|e| panic!("failed to parse JSON log: {e}: {json_line}"));

    assert_eq!(parsed["pipeline-name"], "logging-demo");
    assert_eq!(parsed["fields"]["demo_int"], 42);
    assert_eq!(parsed["fields"]["demo_u64"], Value::from(u64::MAX));
    assert_eq!(parsed["fields"]["demo_float"], 1.23456);
    assert_eq!(parsed["fields"]["demo_neg_float"], -2.5);
    assert_eq!(parsed["fields"]["demo_text"], "sample");
}

fn run_child(mode: &str) -> String {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .env("LOGGING_DEMO_CHILD", mode)
        .env("RUST_LOG", "info")
        .env("FELDERA_LOG_JSON", if mode == "json" { "1" } else { "0" })
        .env("NO_COLOR", "1")
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn child: {e}"));

    assert!(
        output.status.success(),
        "child process exited with {:?}\nstderr: {}\nstdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );

    String::from_utf8(output.stdout).expect("child stdout to be utf-8")
}

fn emit_logs(json: bool) {
    if json {
        std::env::set_var("FELDERA_LOG_JSON", "1");
    } else {
        std::env::remove_var("FELDERA_LOG_JSON");
    }
    // Force INFO output for the demo regardless of upstream defaults.
    std::env::set_var("RUST_LOG", "info");
    std::env::set_var("NO_COLOR", "1");
    init_logging("[logging-demo]".cyan());
    info!("logging demo event");
    info!(
        demo_int = 42i64,
        demo_u64 = u64::MAX,
        demo_float = 1.23456f64,
        demo_neg_float = -2.5f64,
        demo_text = "sample",
        "typed field coverage"
    );
    // Ensure stdout is flushed before the child exits so the parent sees the logs.
    std::io::Write::flush(&mut std::io::stdout()).expect("flush stdout");
}
