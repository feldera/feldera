//! Parse `cargo --message-format=json` stdout into [`RustCompilerMessage`]s.

use crate::db::types::program::RustCompilerMessage;
use serde::Deserialize;

/// Cargo JSON envelope. Extra fields (`package_id`, `target`, …) are ignored.
#[derive(Debug, Deserialize)]
struct CargoMessage {
    reason: String,
    #[serde(default)]
    message: Option<RustcDiagnostic>,
}

/// Subset of rustc's JSON diagnostic (<https://doc.rust-lang.org/rustc/json.html>).
#[derive(Debug, Deserialize)]
struct RustcDiagnostic {
    level: String,
    message: String,
    #[serde(default)]
    code: Option<DiagnosticCode>,
    #[serde(default)]
    spans: Vec<DiagnosticSpan>,
    #[serde(default)]
    rendered: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiagnosticCode {
    code: String,
}

#[derive(Debug, Deserialize)]
struct DiagnosticSpan {
    file_name: String,
    line_start: usize,
    line_end: usize,
    column_start: usize,
    column_end: usize,
    #[serde(default)]
    is_primary: bool,
}

impl RustcDiagnostic {
    fn into_message(self) -> RustCompilerMessage {
        let warning = self.level == "warning";
        let error_type = self
            .code
            .map(|c| c.code)
            .unwrap_or_else(|| self.level.clone());
        let span = self
            .spans
            .iter()
            .find(|s| s.is_primary)
            .or_else(|| self.spans.first());
        let rendered = self.rendered.and_then(|s| (!s.is_empty()).then_some(s));
        match span {
            Some(span) => RustCompilerMessage {
                file: Some(span.file_name.clone()),
                start_line_number: span.line_start,
                start_column: span.column_start,
                end_line_number: span.line_end,
                end_column: span.column_end,
                warning,
                error_type,
                message: self.message,
                rendered,
            },
            None => RustCompilerMessage {
                file: None,
                start_line_number: 0,
                start_column: 0,
                end_line_number: 0,
                end_column: 0,
                warning,
                error_type,
                message: self.message,
                rendered,
            },
        }
    }
}

fn parse_line(line: &str) -> Option<RustCompilerMessage> {
    let line = line.trim();
    // Cargo: treat a line as JSON only if it starts with `{`.
    if !line.starts_with('{') {
        return None;
    }
    let cargo: CargoMessage = serde_json::from_str(line).ok()?;
    if cargo.reason != "compiler-message" {
        return None;
    }
    let diagnostic = cargo.message?;
    match diagnostic.level.as_str() {
        "error" | "warning" => Some(diagnostic.into_message()),
        _ => None,
    }
}

/// rustc errors and warnings from cargo JSON stdout, in order. Other lines are skipped.
pub(crate) fn parse_cargo_json_messages(stdout: &str) -> Vec<RustCompilerMessage> {
    stdout.lines().filter_map(parse_line).collect()
}

#[cfg(test)]
mod tests {
    use super::parse_cargo_json_messages;
    use serde_json::json;

    fn compiler_message(level: &str, extra: serde_json::Value) -> String {
        let mut message = extra;
        message
            .as_object_mut()
            .unwrap()
            .insert("level".to_string(), json!(level));
        json!({
            "reason": "compiler-message",
            "message": message,
        })
        .to_string()
    }

    #[test]
    fn empty_stdout_yields_no_messages() {
        assert!(parse_cargo_json_messages("").is_empty());
        assert!(parse_cargo_json_messages("\n\n").is_empty());
    }

    #[test]
    fn parses_error_with_primary_span() {
        let stdout = compiler_message(
            "error",
            json!({
                "message": "failed to resolve: use of undeclared crate or module `chrnoo`",
                "code": { "code": "E0433" },
                "spans": [{
                    "file_name": "/tmp/udf.rs",
                    "line_start": 3,
                    "line_end": 3,
                    "column_start": 5,
                    "column_end": 11,
                    "is_primary": true
                }],
                "rendered": "error[E0433]: failed to resolve: use of undeclared crate or module `chrnoo`\n"
            }),
        );
        let messages = parse_cargo_json_messages(&stdout);
        assert_eq!(messages.len(), 1);
        let m = &messages[0];
        assert!(!m.warning);
        assert_eq!(m.error_type, "E0433");
        assert_eq!(
            m.message,
            "failed to resolve: use of undeclared crate or module `chrnoo`"
        );
        assert_eq!(m.file.as_deref(), Some("/tmp/udf.rs"));
        assert_eq!(m.start_line_number, 3);
        assert_eq!(m.start_column, 5);
        assert_eq!(m.end_line_number, 3);
        assert_eq!(m.end_column, 11);
        assert!(m.rendered.as_ref().unwrap().contains("E0433"));
    }

    #[test]
    fn parses_warning_without_error_code() {
        let stdout = compiler_message(
            "warning",
            json!({
                "message": "unused variable: `x`",
                "spans": [{
                    "file_name": "/tmp/stubs.rs",
                    "line_start": 8,
                    "line_end": 8,
                    "column_start": 9,
                    "column_end": 10,
                    "is_primary": true
                }]
            }),
        );
        let messages = parse_cargo_json_messages(&stdout);
        assert_eq!(messages.len(), 1);
        assert!(messages[0].warning);
        assert_eq!(messages[0].error_type, "warning");
        assert_eq!(messages[0].file.as_deref(), Some("/tmp/stubs.rs"));
        assert_eq!(messages[0].start_line_number, 8);
    }

    #[test]
    fn uses_primary_span_when_several_exist() {
        let stdout = compiler_message(
            "error",
            json!({
                "message": "mismatched types",
                "code": { "code": "E0308" },
                "spans": [
                    {
                        "file_name": "/tmp/other.rs",
                        "line_start": 1,
                        "line_end": 1,
                        "column_start": 1,
                        "column_end": 2,
                        "is_primary": false
                    },
                    {
                        "file_name": "/tmp/udf.rs",
                        "line_start": 10,
                        "line_end": 10,
                        "column_start": 14,
                        "column_end": 20,
                        "is_primary": true
                    }
                ]
            }),
        );
        let m = &parse_cargo_json_messages(&stdout)[0];
        assert_eq!(m.file.as_deref(), Some("/tmp/udf.rs"));
        assert_eq!(m.start_line_number, 10);
        assert_eq!(m.start_column, 14);
    }

    #[test]
    fn diagnostic_without_span_has_no_file() {
        let stdout = compiler_message(
            "error",
            json!({
                "message": "aborting due to 1 previous error",
                "spans": []
            }),
        );
        let m = &parse_cargo_json_messages(&stdout)[0];
        assert_eq!(m.file, None);
        assert_eq!(m.start_line_number, 0);
        assert_eq!(m.start_column, 0);
        assert_eq!(m.error_type, "error");
    }

    #[test]
    fn ignores_artifacts_notes_and_non_json() {
        let artifact = json!({"reason": "compiler-artifact", "fresh": true}).to_string();
        let note = compiler_message("note", json!({"message": "expected i32"}));
        let help = compiler_message("help", json!({"message": "consider adding a semicolon"}));
        let stdout = [
            "   Compiling pipeline v0.1.0",
            artifact.as_str(),
            "{this is not json",
            note.as_str(),
            help.as_str(),
            "",
        ]
        .join("\n");
        assert!(parse_cargo_json_messages(&stdout).is_empty());
    }

    #[test]
    fn keeps_errors_and_warnings_in_order_among_noise() {
        let warning = compiler_message(
            "warning",
            json!({
                "message": "unused import",
                "spans": [{
                    "file_name": "/tmp/udf.rs",
                    "line_start": 1,
                    "line_end": 1,
                    "column_start": 5,
                    "column_end": 8,
                    "is_primary": true
                }]
            }),
        );
        let error = compiler_message(
            "error",
            json!({
                "message": "cannot find value `x` in this scope",
                "code": { "code": "E0425" },
                "spans": [{
                    "file_name": "/tmp/udf.rs",
                    "line_start": 4,
                    "line_end": 4,
                    "column_start": 9,
                    "column_end": 10,
                    "is_primary": true
                }]
            }),
        );
        let stdout = format!(
            "fresh noise\n{}\n{}\n{}\n",
            json!({"reason": "build-finished", "success": false}),
            warning,
            error
        );
        let messages = parse_cargo_json_messages(&stdout);
        assert_eq!(messages.len(), 2);
        assert!(messages[0].warning);
        assert_eq!(messages[0].message, "unused import");
        assert!(!messages[1].warning);
        assert_eq!(messages[1].error_type, "E0425");
    }
}
