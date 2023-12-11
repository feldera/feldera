//! Source position representation.
//! Used for reporting run-time errors.

// Currently Calcite does not provide source-position, but we have
// opened submitted patches.  Once they are accepted this will be much
// more useful.

use std::fmt;

#[derive(Default, Debug)]
pub struct SourcePosition {
    // Source lines are counted from 1, so a value a 0 for row
    // indicates "unknown".
    pub line: u32,
    pub column: u32,
}

#[derive(Default, Debug)]
pub struct SourcePositionRange {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

impl SourcePosition {
    pub fn new(line: u32, column: u32) -> Self {
        Self { line, column }
    }

    pub fn isValid(&self) -> bool {
        self.line > 0
    }
}

impl fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.isValid() {
            write!(f, "{}:{}", self.line, self.column)
        } else {
            write!(f, "")
        }
    }
}

impl SourcePositionRange {
    pub fn new(start: SourcePosition, end: SourcePosition) -> Self {
        Self { start, end }
    }

    pub fn isValid(&self) -> bool {
        self.start.isValid()
    }
}

impl fmt::Display for SourcePositionRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.isValid() {
            write!(f, "{}-{}", self.start, self.end)
        } else {
            write!(f, "")
        }
    }
}
