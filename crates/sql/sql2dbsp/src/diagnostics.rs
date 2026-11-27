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
    let lines: Vec<_> = sql.lines().collect();
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
    message["error_type"] = "Error parsing SQL".into();
    message["message"] = diagnostic.message.into();
    message["snippet"] = range.snippet(&lines)?.into();
    Ok(Some(message))
}

/// Advisory front-end result for editors. A valid SQL construct outside the
/// binder's coverage needs a different verdict from a located compiler error.
pub fn preview(sql: &str) -> Result<Value, Box<dyn Error>> {
    match feldera_sql_binder::preview_schema(sql) {
        Ok(schema) => {
            Ok(json!({"advisory":true, "status":"bound", "schema":schema, "messages":[]}))
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
