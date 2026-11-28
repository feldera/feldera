//! Compiler source coordinates are one-based inclusive UTF-16 offsets.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt::Write;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourceRange {
    pub start_line_number: i32,
    pub start_column: i32,
    pub end_line_number: i32,
    pub end_column: i32,
}

impl SourceRange {
    pub fn is_valid(self) -> bool {
        self.start_line_number > 0
            && self.start_column > 0
            && self.end_line_number > 0
            && self.end_column > 0
    }

    /// Convert a parser span with one-based character columns and an exclusive end.
    pub fn from_character_span(lines: &[&str], start: (u64, u64), end: (u64, u64)) -> Option<Self> {
        fn offset(lines: &[&str], (line, column): (u64, u64)) -> Option<(i32, i32)> {
            let text = lines.get(usize::try_from(line.checked_sub(1)?).ok()?)?;
            let characters = usize::try_from(column.checked_sub(1)?).ok()?;
            let mut units = 0usize;
            let mut chars = text.chars();
            for _ in 0..characters {
                units = units.checked_add(chars.next()?.len_utf16())?;
            }
            Some((i32::try_from(line).ok()?, i32::try_from(units).ok()?))
        }
        let (start_line_number, start_offset) = offset(lines, start)?;
        // An exclusive end at the start of a later line includes the previous
        // newline. The compiler represents it one UTF-16 unit past that line.
        let (end_line_number, end_offset) = if end.0 > start.0 && end.1 == 1 {
            let line = end.0.checked_sub(1)?;
            let text = lines.get(usize::try_from(line.checked_sub(1)?).ok()?)?;
            (
                i32::try_from(line).ok()?,
                i32::try_from(text.encode_utf16().count())
                    .ok()?
                    .checked_add(1)?,
            )
        } else {
            offset(lines, end)?
        };
        if (end_line_number, end_offset) < (start_line_number, start_offset) {
            return None;
        }
        let start_column = start_offset.checked_add(1)?;
        let end_column = if start == end {
            start_column
        } else {
            end_offset
        };
        Some(Self {
            start_line_number,
            start_column,
            end_line_number,
            end_column,
        })
    }

    /// Decorate the named source lines using the compiler's public snippet format.
    pub fn snippet(self, lines: &[&str]) -> Result<String, &'static str> {
        let mut text = String::new();
        if !self.is_valid() {
            return Ok(text);
        }
        let first = self.start_line_number as usize - 1;
        let last = self.end_line_number as usize - 1;
        if first == last && first >= lines.len() {
            return Ok(text);
        }
        if first >= lines.len() || last < first || last > lines.len() {
            return Err("invalid line range in checkpoint diagnostic");
        }
        let append_line = |output: &mut String, index: usize| {
            writeln!(output, "{:5}|{}", index + 1, lines[index]).expect("string write");
        };
        if first == last {
            let column = self.start_column as usize - 1;
            let end = self.end_column as usize;
            // Calcite columns are UTF-16 code units, inclusive, with one extra
            // position at end-of-line. Check the range before allocating padding.
            if end <= column || end > lines[first].encode_utf16().count() + 1 {
                return Err("invalid column range in checkpoint diagnostic");
            }
            if first > 0 && first + 1 == lines.len() {
                append_line(&mut text, first - 1);
            }
            append_line(&mut text, first);
            text.extend(std::iter::repeat_n(' ', column + 6));
            text.extend(std::iter::repeat_n('^', end - column));
            text.push('\n');
            if first + 1 < lines.len() {
                append_line(&mut text, first + 1);
            }
        } else {
            let last = last.min(lines.len() - 1);
            if last - first < 5 {
                for index in first..=last {
                    append_line(&mut text, index);
                }
            } else {
                append_line(&mut text, first);
                append_line(&mut text, first + 1);
                text.push_str("      ...\n");
                append_line(&mut text, last);
            }
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::SourceRange;

    #[test]
    fn converts_supplementary_characters_to_inclusive_utf16_columns() {
        let lines = ["🌱abc"];
        let range = SourceRange::from_character_span(&lines, (1, 2), (1, 4)).unwrap();
        assert_eq!((range.start_column, range.end_column), (3, 4));
        assert_eq!(range.snippet(&lines).unwrap(), "    1|🌱abc\n        ^^\n");
        assert!(SourceRange::from_character_span(&lines, (1, 7), (1, 8)).is_none());
    }

    #[test]
    fn span_ending_at_next_line_includes_the_newline() {
        let lines = ["🌱", "abc"];
        for lines in [&lines[..], &lines[..1]] {
            let range = SourceRange::from_character_span(lines, (1, 1), (2, 1)).unwrap();
            assert_eq!((range.end_line_number, range.end_column), (1, 3));
            assert!(range.snippet(lines).unwrap().contains("^^^"));
        }
    }

    #[test]
    fn long_multiline_snippets_show_the_first_two_and_last_lines() {
        let lines = ["a", "b", "c", "d", "e", "f", "g"];
        let range = SourceRange::from_character_span(&lines, (1, 1), (7, 2)).unwrap();
        assert_eq!(
            range.snippet(&lines).unwrap(),
            "    1|a\n    2|b\n      ...\n    7|g\n"
        );
    }
}
