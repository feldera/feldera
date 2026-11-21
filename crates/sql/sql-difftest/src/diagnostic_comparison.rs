use crate::files::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The eight fields emitted by Java's CompilerMessages and consumed by the
/// pipeline manager. A zero line and column denote a diagnostic without a range.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Diagnostic<'a> {
    warning: bool,
    error_type: &'a str,
    start_line_number: u32,
    start_column: u32,
    end_line_number: u32,
    end_column: u32,
    message: &'a str,
    snippet: &'a str,
}

#[derive(Default, Serialize)]
pub(crate) struct DiagnosticMetrics {
    matched_diagnostics: usize,
    messages_changed: usize,
    end_positions_changed: usize,
}

pub(crate) fn compare(
    probe: &Value,
    expected: &Value,
    actual: &Value,
    metrics: &mut DiagnosticMetrics,
) -> Result<bool, Error> {
    let decode = |side: &str, value| {
        Vec::<Diagnostic<'_>>::deserialize(value).map_err(|error| {
            Error::Difference(format!(
                "probe {probe}: invalid {side} diagnostics: {error}"
            ))
        })
    };
    let expected = decode("expected", expected)?;
    let actual = decode("actual", actual)?;
    let mut equal = expected.len() == actual.len();
    for (left, right) in expected.iter().zip(actual.iter()) {
        let same_diagnostic = left.warning == right.warning
            && left.error_type == right.error_type
            && left.start_line_number == right.start_line_number
            && left.start_column == right.start_column
            && left.snippet == right.snippet;
        equal &= same_diagnostic;
        if same_diagnostic {
            metrics.matched_diagnostics += 1;
            metrics.messages_changed += usize::from(left.message != right.message);
            metrics.end_positions_changed += usize::from(
                (left.end_line_number, left.end_column)
                    != (right.end_line_number, right.end_column),
            );
        }
    }
    Ok(equal)
}
