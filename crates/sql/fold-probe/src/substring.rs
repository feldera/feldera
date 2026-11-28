//! SQL character and byte slices use the same one-based index arithmetic.

use std::ops::Range;

#[derive(Clone, Copy)]
pub enum StringSlice {
    Left(i128),
    Right(i128),
    Substring { start: i128, length: Option<i128> },
}

impl StringSlice {
    /// A negative SQL SUBSTRING length raises a runtime error.
    pub fn range(self, size: usize) -> Option<Range<usize>> {
        let clip = |index: i128| index.clamp(0, size as i128) as usize;
        Some(match self {
            Self::Left(count) => 0..clip(count),
            Self::Right(count) => size - clip(count)..size,
            Self::Substring { start, length } => {
                let offset = start.saturating_sub(1);
                let end = match length {
                    None => size,
                    Some(n) if n >= 0 => clip(offset.saturating_add(n)),
                    Some(_) => return None,
                };
                clip(offset)..end
            }
        })
    }

    pub fn text(self, value: &str) -> Option<String> {
        let range = self.range(value.chars().count())?;
        Some(value.chars().skip(range.start).take(range.len()).collect())
    }

    pub fn bytes(self, value: &[u8]) -> Option<Vec<u8>> {
        Some(value[self.range(value.len())?].to_vec())
    }
}
