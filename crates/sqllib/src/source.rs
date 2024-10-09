//! Source position representation.
//! Used for reporting run-time errors.

use std::fmt;

#[doc(hidden)]
#[derive(Default, Debug)]
pub struct SourcePosition {
    // Source lines are counted from 1, so a value a 0 for row
    // indicates "unknown".
    pub line: u32,
    pub column: u32,
}

#[doc(hidden)]
#[derive(Default, Debug)]
pub struct SourcePositionRange {
    pub start: SourcePosition,
    pub end: SourcePosition,
}

#[doc(hidden)]
impl SourcePosition {
    #[doc(hidden)]
    pub fn new(line: u32, column: u32) -> Self {
        Self { line, column }
    }

    #[doc(hidden)]
    pub fn isValid(&self) -> bool {
        self.line > 0
    }
}

impl fmt::Display for SourcePosition {
    #[doc(hidden)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.isValid() {
            write!(f, "{}:{}", self.line, self.column)
        } else {
            write!(f, "")
        }
    }
}

impl SourcePositionRange {
    #[doc(hidden)]
    pub fn new(start: SourcePosition, end: SourcePosition) -> Self {
        Self { start, end }
    }

    #[doc(hidden)]
    pub fn isValid(&self) -> bool {
        self.start.isValid()
    }
}

impl fmt::Display for SourcePositionRange {
    #[doc(hidden)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.isValid() {
            write!(f, "{}-{}", self.start, self.end)
        } else {
            write!(f, "")
        }
    }
}
