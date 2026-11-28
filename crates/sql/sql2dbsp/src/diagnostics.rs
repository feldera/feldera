//! Render the public SQL messages saved in a compiler checkpoint.

use feldera_sql_source::SourceRange;
use serde::Deserialize;
use serde_json::{json, Value};
use std::error::Error;

#[derive(Deserialize)]
struct Message<'a> {
    range: SourceRange,
    warning: bool,
    #[serde(rename = "type")]
    error_type: &'a str,
    message: &'a str,
}

pub fn render(messages: &Value, source: &Value) -> Result<Value, Box<dyn Error>> {
    let lines = Vec::<&str>::deserialize(source)?;
    let messages = Vec::<Message>::deserialize(messages)?;
    let mut result = Vec::with_capacity(messages.len());
    for message in messages {
        let mut public = json!(message.range);
        public["warning"] = message.warning.into();
        public["error_type"] = message.error_type.into();
        public["message"] = message.message.into();
        public["snippet"] = message.range.snippet(&lines)?.into();
        result.push(public);
    }
    Ok(result.into())
}

/// Emit the structured message used by the platform for a raw SQL parse failure.
pub fn syntax(sql: &str) -> Result<Option<Value>, Box<dyn Error>> {
    let Some(diagnostic) = feldera_sql_parser::syntax_diagnostic(sql)? else {
        return Ok(None);
    };
    let lines: Vec<_> = sql.split('\n').collect();
    let range = diagnostic
        .span
        .and_then(|s| {
            SourceRange::from_character_span(
                &lines,
                (s.start.line, s.start.column),
                (s.end.line, s.end.column),
            )
        })
        .unwrap_or_default();
    let mut message = json!(range);
    message["warning"] = false.into();
    message["error_type"] = diagnostic.error_type.into();
    message["message"] = diagnostic.message.into();
    // Java keeps CR in source lines and drops trailing empty strings from split.
    let visible = lines
        .iter()
        .rposition(|line| !line.is_empty())
        .map_or(0, |i| i + 1);
    message["snippet"] = range.snippet(&lines[..visible])?.into();
    Ok(Some(message))
}

/// Advisory front-end result for editors. A valid SQL construct outside the
/// binder's coverage needs a different verdict from a located compiler error.
pub fn preview(sql: &str) -> Result<Value, Box<dyn Error>> {
    match feldera_sql_binder::preview_schema_and_diagnostics(sql) {
        Ok((schema, messages)) => {
            Ok(json!({"advisory":true, "status":"bound", "schema":schema, "messages":messages}))
        }
        Err(failure) => {
            let diagnostic = if failure.is_parse_error() {
                syntax(sql)?
            } else {
                failure
                    .compiler_diagnostic()
                    .map(serde_json::to_value)
                    .transpose()?
            };
            Ok(match diagnostic {
                Some(message) => json!({"advisory":true, "status":"error", "messages":[message]}),
                None => {
                    json!({"advisory":true, "status":"unsupported", "reason":failure.to_string(), "messages":[]})
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::syntax;

    #[test]
    fn repeated_column_attributes_report_the_second_declaration() {
        for (attributes, column, end) in [
            ("DEFAULT 1 DEFAULT (2+3)", 19, 23),
            ("PRIMARY KEY PRIMARY   KEY", 13, 25),
            ("LATENESS 1 LATENESS 2", 21, 21),
        ] {
            let sql = format!("CREATE TABLE t(x INT\n{attributes})");
            let msg = syntax(&sql).unwrap().expect("duplicate column attribute");
            assert_eq!(msg["error_type"], "Compilation error");
            assert_eq!(msg["start_line_number"], 2);
            assert_eq!(msg["start_column"], column, "{sql}");
            assert_eq!(msg["end_column"], end, "{sql}");
        }
    }

    #[test]
    fn eof_uses_the_last_utf16_column_and_the_original_source_lines() {
        for (tail, line, column) in [("", 1, 29), (" ", 1, 30), ("\r\n", 1, 31), ("\n\n", 2, 1)] {
            let source = format!("SELECT '🦀' FETCH FIRST 1 ROW{tail}");
            let message = syntax(&source).unwrap().unwrap();
            assert_eq!(message["start_line_number"], line);
            assert_eq!(message["start_column"], column, "{source:?}");
            if tail == "\n\n" {
                assert_eq!(message["snippet"], "");
            }
        }
    }
}
