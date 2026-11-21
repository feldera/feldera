//! SQL literal origin and SET warning policy, separate from config validation.

use crate::Warning;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub program: String,
    /// Position of the opening quote, as in Java's SqlFragment.
    pub line: usize,
    pub column: usize,
    pub policy: Policy,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    Enabled,
    Suppressed,
    Promoted,
    SuppressedAndPromoted,
}

impl Source {
    pub(crate) fn apply(&self, warnings: &mut Vec<Warning>) -> bool {
        if matches!(
            self.policy,
            Policy::Suppressed | Policy::SuppressedAndPromoted
        ) {
            warnings.clear();
            return true;
        }
        for warning in warnings.iter_mut() {
            warning.warning = !matches!(self.policy, Policy::Promoted);
            let pos = &mut warning.position;
            if pos.start_line_number != 0 {
                if pos.start_line_number == 1 {
                    pos.start_column += self.column;
                    pos.end_column += self.column;
                }
                pos.start_line_number += self.line - 1;
                pos.end_line_number += self.line - 1;
            }
            warning.snippet = Some(self.snippet(pos.start_line_number, pos.start_column));
        }
        let accepted = warnings.iter().all(|warning| warning.warning);
        if !accepted {
            // The SQL driver adds this terminal diagnostic after promoted
            // configuration errors; it is part of message-count parity.
            warnings.push(Warning {
                warning: false,
                continuation: false,
                error_type: "Compilation error",
                message: "Stopping compilation due to errors".into(),
                position: Default::default(),
                snippet: Some(String::new()),
            });
        }
        accepted
    }

    fn snippet(&self, line: usize, column: usize) -> String {
        if line == 0 || column == 0 {
            return String::new();
        }
        let lines: Vec<_> = self.program.split_terminator('\n').collect();
        let index = line - 1;
        let Some(text) = lines.get(index) else {
            return String::new();
        };
        let mut result = String::new();
        if line == lines.len() && index > 0 {
            result.push_str(&format!("{index:5}|{}\n", lines[index - 1]));
        }
        result.push_str(&format!("{line:5}|{text}\n{}^\n", " ".repeat(column + 5)));
        if let Some(next) = lines.get(index + 1) {
            result.push_str(&format!("{:5}|{next}\n", line + 1));
        }
        result
    }
}
