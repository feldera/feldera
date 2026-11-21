//! Token locations in the original JSON, in Jackson's one-based UTF-16 columns.
//! The JSON parser validates syntax before this bounded traversal runs.

use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Serialize)]
pub struct Position {
    pub start_line_number: usize,
    pub start_column: usize,
    pub end_line_number: usize,
    pub end_column: usize,
}

pub(crate) struct Locations(BTreeMap<String, (Position, Position)>);

impl Locations {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut scanner = Scanner {
            raw,
            offset: 0,
            locations: BTreeMap::new(),
        };
        scanner.value("", None, 0)?;
        Ok(Self(scanner.locations))
    }

    pub fn key(&self, path: &str) -> Option<Position> {
        self.0.get(path).map(|p| p.0)
    }
    pub fn value(&self, path: &str) -> Option<Position> {
        self.0.get(path).map(|p| p.1)
    }
}

struct Scanner<'a> {
    raw: &'a str,
    offset: usize,
    locations: BTreeMap<String, (Position, Position)>,
}

impl Scanner<'_> {
    fn whitespace(&mut self) {
        while self
            .raw
            .as_bytes()
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }

    fn position(&self) -> Position {
        let prefix = &self.raw[..self.offset];
        let line = prefix.bytes().filter(|b| *b == b'\n').count() + 1;
        let column = prefix
            .rsplit('\n')
            .next()
            .unwrap_or("")
            .encode_utf16()
            .count()
            + 1;
        Position {
            start_line_number: line,
            end_line_number: line,
            start_column: column,
            end_column: column,
        }
    }

    fn string(&mut self) -> Result<String, String> {
        let start = self.offset;
        self.offset += 1;
        while let Some(&byte) = self.raw.as_bytes().get(self.offset) {
            self.offset += 1;
            match byte {
                b'\\' => self.offset += 1,
                b'"' => {
                    return serde_json::from_str(&self.raw[start..self.offset])
                        .map_err(|e| e.to_string())
                }
                _ => {}
            }
        }
        Err("unterminated JSON string".into())
    }

    fn value(&mut self, path: &str, key: Option<Position>, depth: usize) -> Result<(), String> {
        if depth > 100 {
            return Err("JSON nesting exceeds prototype limit (100)".into());
        }
        self.whitespace();
        let position = self.position();
        self.locations
            .entry(path.to_owned())
            .or_insert((key.unwrap_or(position), position));
        match self.raw.as_bytes().get(self.offset) {
            Some(b'{') => {
                self.offset += 1;
                self.whitespace();
                while self.raw.as_bytes().get(self.offset) != Some(&b'}') {
                    let key_position = self.position();
                    let name = self.string()?.replace('~', "~0").replace('/', "~1");
                    self.whitespace();
                    self.offset += 1; // ':'; serde already checked the syntax.
                    self.value(&format!("{path}/{name}"), Some(key_position), depth + 1)?;
                    self.whitespace();
                    if self.raw.as_bytes().get(self.offset) != Some(&b',') {
                        break;
                    }
                    self.offset += 1;
                    self.whitespace();
                }
                self.offset += 1;
            }
            Some(b'[') => {
                self.offset += 1;
                self.whitespace();
                let mut index = 0;
                while self.raw.as_bytes().get(self.offset) != Some(&b']') {
                    self.value(&format!("{path}/{index}"), None, depth + 1)?;
                    index += 1;
                    self.whitespace();
                    if self.raw.as_bytes().get(self.offset) != Some(&b',') {
                        break;
                    }
                    self.offset += 1;
                    self.whitespace();
                }
                self.offset += 1;
            }
            Some(b'"') => {
                self.string()?;
            }
            Some(_) => {
                while self
                    .raw
                    .as_bytes()
                    .get(self.offset)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.offset += 1;
                }
            }
            None => return Err("missing JSON value".into()),
        }
        Ok(())
    }
}
