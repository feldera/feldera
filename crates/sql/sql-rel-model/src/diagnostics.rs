//! Accepted frontend warnings interleaved with statement lowering. The timeline
//! contains no lowering diagnostics: those must be produced by the consumer.
use crate::{
    TypeTable, ValidationError, ValidationLimits,
    source::{Origin, SourceId},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, ValidationError>;
fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: "diagnostics".into(),
            message,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendDiagnostics {
    pub before: Vec<DiagnosticBoundary>,
    pub after: DiagnosticBoundary,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticBoundary {
    pub context: Origin,
    pub messages: Vec<FrontendWarning>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendWarning {
    pub context: Origin,
    pub range: Origin,
    pub continuation: bool,
    #[serde(rename = "type")]
    pub category: String,
    pub message: String,
}

impl FrontendDiagnostics {
    pub(crate) fn validate(
        &self,
        statements: usize,
        types: &TypeTable,
        limits: ValidationLimits,
    ) -> Result<()> {
        require(
            self.before.len() == statements,
            "diagnostic boundaries differ from statement count",
        )?;
        let mut messages = limits.types;
        let mut bytes = limits.source_bytes;
        let lines: Vec<usize> = types.sources.first().map_or_else(Vec::new, |source| {
            source
                .text
                .split('\n')
                .map(|line| line.encode_utf16().count())
                .collect()
        });
        let origin = |value: &Origin| {
            require(
                value.source().is_none_or(|source| source == SourceId(0)),
                "diagnostics must use the user source",
            )?;
            types.origin(value, "diagnostics.origin")?;
            if let Origin::Source { start, end, .. } | Origin::Generated { start, end, .. } = value
            {
                for position in [start, end] {
                    if position.line != 0 {
                        let length = lines.get(position.line as usize - 1);
                        require(
                            length.is_some_and(|length| position.column as usize <= length + 1),
                            "diagnostic position is outside its source",
                        )?;
                    }
                }
            }
            Ok(())
        };
        for boundary in self.before.iter().chain(std::iter::once(&self.after)) {
            origin(&boundary.context)?;
            for warning in &boundary.messages {
                require(messages > 0, "frontend diagnostic count exceeds limit")?;
                messages -= 1;
                origin(&warning.context)?;
                origin(&warning.range)?;
                for text in [&warning.category, &warning.message] {
                    require(
                        text.len() <= bytes,
                        "frontend diagnostic text exceeds limit",
                    )?;
                    bytes -= text.len();
                }
            }
        }
        Ok(())
    }
}
