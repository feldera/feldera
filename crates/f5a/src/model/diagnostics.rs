//! Error diagnostics assembled from a pipeline descriptor.
//!
//! A broken pipeline scatters its evidence: `deployment_error` for runtime
//! failures, `program_error.system_error` when the compiler service itself
//! failed, `program_error.sql_compilation.messages` for SQL problems, and
//! `program_error.rust_compilation.messages` (or `stderr`) for Rust build
//! failures. The Logs view shows all of them above the runtime log stream.

use serde_json::Value;

use super::json;
use super::text::terminal_safe_multiline;

/// How prominently a section renders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// One titled block of diagnostic text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticSection {
    pub title: String,
    pub severity: Severity,
    pub body: String,
}

/// Extract every diagnostic a full pipeline descriptor carries.
pub fn from_pipeline_json(descriptor: &Value) -> Vec<DiagnosticSection> {
    let mut sections = Vec::new();
    if let Some(section) = deployment_error(descriptor) {
        sections.push(section);
    }
    let program_error = descriptor
        .get("program_error")
        .cloned()
        .unwrap_or(Value::Null);
    if let Some(section) = system_error(&program_error) {
        sections.push(section);
    }
    sections.extend(sql_sections(&program_error));
    if let Some(section) = rust_section(&program_error) {
        sections.push(section);
    }
    sections
}

fn deployment_error(descriptor: &Value) -> Option<DiagnosticSection> {
    let error = descriptor.get("deployment_error")?;
    if error.is_null() {
        return None;
    }
    let mut body = json::string(error, "message").unwrap_or_default();
    if let Some(details) = error.get("details").filter(|details| !details.is_null()) {
        let details = serde_json::to_string_pretty(details).unwrap_or_default();
        if details != "{}" {
            body.push_str("\n\ndetails:\n");
            body.push_str(&details);
        }
    }
    (!body.trim().is_empty()).then(|| DiagnosticSection {
        title: "DEPLOYMENT ERROR".to_string(),
        severity: Severity::Error,
        body: terminal_safe_multiline(body.trim()),
    })
}

fn system_error(program_error: &Value) -> Option<DiagnosticSection> {
    let message = json::string(program_error, "system_error")?;
    (!message.trim().is_empty()).then(|| DiagnosticSection {
        title: "SYSTEM ERROR".to_string(),
        severity: Severity::Error,
        body: terminal_safe_multiline(message.trim()),
    })
}

fn sql_sections(program_error: &Value) -> Vec<DiagnosticSection> {
    let messages = program_error
        .get("sql_compilation")
        .map(|compilation| json::array(compilation, "messages"))
        .unwrap_or_default();
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    for message in messages {
        let rendered = render_sql_message(message);
        if json::boolean(message, "warning") {
            warnings.push(rendered);
        } else {
            errors.push(rendered);
        }
    }
    let mut sections = Vec::new();
    if !errors.is_empty() {
        sections.push(DiagnosticSection {
            title: "SQL COMPILATION ERRORS".to_string(),
            severity: Severity::Error,
            body: terminal_safe_multiline(&errors.join("\n\n")),
        });
    }
    if !warnings.is_empty() {
        sections.push(DiagnosticSection {
            title: "SQL COMPILATION WARNINGS".to_string(),
            severity: Severity::Warning,
            body: terminal_safe_multiline(&warnings.join("\n\n")),
        });
    }
    sections
}

fn render_sql_message(message: &Value) -> String {
    let line = json::integer_or_zero(message, "start_line_number");
    let column = json::integer_or_zero(message, "start_column");
    let error_type = json::string(message, "error_type").unwrap_or_default();
    let text = json::string(message, "message").unwrap_or_default();
    let mut rendered = format!("L{line}:{column} {error_type}: {text}");
    if let Some(snippet) = json::string(message, "snippet") {
        rendered.push('\n');
        rendered.push_str(snippet.trim_end());
    }
    rendered
}

fn rust_section(program_error: &Value) -> Option<DiagnosticSection> {
    let compilation = program_error.get("rust_compilation")?;
    let exit_code = json::integer(compilation, "exit_code")?;
    if exit_code == 0 {
        return None;
    }
    let errors: Vec<String> = json::array(compilation, "messages")
        .iter()
        .filter(|message| !json::boolean(message, "warning"))
        .map(render_rust_message)
        .collect();
    // Cargo-level failures (a bad `udf.toml`, a build script) carry no rustc
    // diagnostics, so only stderr explains them.
    let body = if errors.is_empty() {
        interesting_rust_tail(&json::string(compilation, "stderr").unwrap_or_default())
    } else {
        errors.join("\n\n")
    };
    Some(DiagnosticSection {
        title: format!("RUST COMPILATION FAILED (exit {exit_code})"),
        severity: Severity::Error,
        body: terminal_safe_multiline(&body),
    })
}

fn render_rust_message(message: &Value) -> String {
    if let Some(rendered) = json::string(message, "rendered").filter(|text| !text.trim().is_empty())
    {
        return rendered.trim_end().to_string();
    }
    let error_type = json::string(message, "error_type").unwrap_or_default();
    let text = json::string(message, "message").unwrap_or_default();
    match json::string(message, "file") {
        Some(file) => {
            let line = json::integer_or_zero(message, "start_line_number");
            let column = json::integer_or_zero(message, "start_column");
            format!("{error_type}: {text} ({file}:{line}:{column})")
        }
        None => format!("{error_type}: {text}"),
    }
}

/// Cargo output is thousands of `Compiling` lines; keep the part that
/// explains the failure.
fn interesting_rust_tail(stderr: &str) -> String {
    const MAX_LINES: usize = 200;
    const FALLBACK_TAIL: usize = 60;
    let lines: Vec<&str> = stderr.lines().collect();
    let first_error = lines
        .iter()
        .position(|line| line.starts_with("error") || line.contains("error["));
    let selected: Vec<&str> = match first_error {
        Some(index) => lines[index..].iter().take(MAX_LINES).copied().collect(),
        None => lines
            .iter()
            .skip(lines.len().saturating_sub(FALLBACK_TAIL))
            .copied()
            .collect(),
    };
    selected.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{Severity, from_pipeline_json, interesting_rust_tail};
    use serde_json::json;

    #[test]
    fn a_healthy_pipeline_has_no_sections() {
        assert!(from_pipeline_json(&json!({"name": "ok", "deployment_error": null})).is_empty());
        assert!(
            from_pipeline_json(&json!({
                "program_error": {"rust_compilation": {"exit_code": 0, "stderr": ""}}
            }))
            .is_empty()
        );
    }

    #[test]
    fn deployment_errors_include_their_details() {
        let sections = from_pipeline_json(&json!({
            "deployment_error": {
                "message": "worker crashed",
                "details": {"exit_code": 137}
            }
        }));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].title, "DEPLOYMENT ERROR");
        assert_eq!(sections[0].severity, Severity::Error);
        assert!(sections[0].body.contains("worker crashed"));
        assert!(sections[0].body.contains("137"));
    }

    #[test]
    fn system_errors_explain_a_failed_compilation() {
        let sections = from_pipeline_json(&json!({
            "program_error": {
                "sql_compilation": null,
                "rust_compilation": null,
                "system_error": "compiler server unreachable\n"
            }
        }));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].title, "SYSTEM ERROR");
        assert_eq!(sections[0].severity, Severity::Error);
        assert_eq!(sections[0].body, "compiler server unreachable");
        assert!(from_pipeline_json(&json!({"program_error": {"system_error": null}})).is_empty());
    }

    #[test]
    fn sql_messages_split_into_errors_and_warnings() {
        let sections = from_pipeline_json(&json!({
            "program_error": {"sql_compilation": {"exit_code": 1, "messages": [
                {"start_line_number": 3, "start_column": 8, "warning": false,
                 "error_type": "Syntax error", "message": "bad token",
                 "snippet": "    3| SELECT bogus\n"},
                {"start_line_number": 1, "start_column": 1, "warning": true,
                 "error_type": "Unnamed connector", "message": "name it"}
            ]}}
        }));
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].title, "SQL COMPILATION ERRORS");
        assert!(sections[0].body.contains("L3:8 Syntax error: bad token"));
        assert!(sections[0].body.contains("SELECT bogus"));
        assert_eq!(sections[1].severity, Severity::Warning);
        assert!(sections[1].body.contains("name it"));
    }

    #[test]
    fn rust_failures_keep_the_error_not_the_build_log() {
        let stderr = (0..500)
            .map(|index| format!("   Compiling crate{index}"))
            .chain([
                "error[E0432]: unresolved import `dbsp::SetHandle`".to_string(),
                "  --> src/lib.rs:42:16".to_string(),
            ])
            .collect::<Vec<_>>()
            .join("\n");
        let sections = from_pipeline_json(&json!({
            "program_error": {"rust_compilation": {"exit_code": 101, "stderr": stderr}}
        }));
        assert_eq!(sections.len(), 1);
        assert!(sections[0].title.contains("exit 101"));
        assert!(sections[0].body.starts_with("error[E0432]"));
        assert!(!sections[0].body.contains("Compiling crate1\n"));
    }

    #[test]
    fn rust_failures_show_the_parsed_compiler_errors() {
        let sections = from_pipeline_json(&json!({
            "program_error": {"rust_compilation": {
                "exit_code": 101,
                "stdout": "",
                "stderr": "   Compiling udf\nerror: could not compile `udf` due to 2 previous errors",
                "messages": [
                    {"warning": false, "error_type": "E0433", "message": "unresolved `chrnoo`",
                     "file": "src/udf.rs", "start_line_number": 3, "start_column": 5,
                     "rendered": "error[E0433]: unresolved `chrnoo`\n --> src/udf.rs:3:5\n"},
                    {"warning": false, "error_type": "E0308", "message": "mismatched types",
                     "file": "src/udf.rs", "start_line_number": 9, "start_column": 1,
                     "rendered": null},
                    {"warning": true, "error_type": "unused_imports", "message": "unused import",
                     "rendered": "warning: unused import"}
                ]
            }}
        }));
        assert_eq!(sections.len(), 1);
        assert_eq!(
            sections[0].body,
            "error[E0433]: unresolved `chrnoo`\n --> src/udf.rs:3:5\n\n\
             E0308: mismatched types (src/udf.rs:9:1)"
        );
    }

    #[test]
    fn rust_output_without_an_error_line_keeps_the_tail() {
        let stderr = (0..100)
            .map(|index| format!("line{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let tail = interesting_rust_tail(&stderr);
        assert!(tail.starts_with("line40"));
        assert!(tail.ends_with("line99"));
    }
}
