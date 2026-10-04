//! Find where a relation and its connectors are declared in a SQL program,
//! so the console can jump from a connector to its configuration.
//!
//! Connectors live in a string literal of the relation's `WITH` clause:
//!
//! ```sql
//! CREATE TABLE orders (id INT) WITH ('connectors' = '[{"name": "kafka", ...}]');
//! ```
//!
//! A light lexer skips comments and literals to find the statement; the
//! literal's JSON array is parsed to pick the connector, and a brace scan of
//! the raw literal maps that array element back to source lines.

use serde_json::Value;

/// Source lines of a match, 1-based and inclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineSpan {
    pub first: usize,
    pub last: usize,
}

/// The most precise declaration found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Located {
    /// The connector's object inside the `connectors` property.
    Connector(LineSpan),
    /// The relation's whole `connectors` property.
    Connectors(LineSpan),
    /// The relation's `CREATE` header; it declares no `connectors` property.
    Relation(LineSpan),
}

/// Locate `relation` and, when given, one of its connectors. Unnamed
/// connectors go by `unnamed-N`, their position in the array. `None` when
/// the program declares no such table or view.
///
/// ```
/// use f5a::model::sql_locate::{LineSpan, Located, locate};
///
/// let sql = "CREATE TABLE t (x INT) WITH ('connectors' = '[\n  {\"name\": \"a\"}\n]');";
/// assert_eq!(locate(sql, "t", Some("a")), Some(Located::Connector(LineSpan { first: 2, last: 2 })));
/// assert_eq!(locate(sql, "missing", None), None);
/// ```
pub fn locate(sql: &str, relation: &str, connector: Option<&str>) -> Option<Located> {
    let tokens = lex(sql);
    let statement = find_statement(&tokens, relation)?;
    let line_of = |offset: usize| sql[..offset].matches('\n').count() + 1;
    let header = LineSpan {
        first: line_of(tokens[statement.create].start),
        last: line_of(tokens[statement.name].start),
    };
    let Some((literal_start, literal_end)) = connectors_literal(sql, &tokens[statement.body()])
    else {
        return Some(Located::Relation(header));
    };
    let property = LineSpan {
        first: line_of(literal_start),
        last: line_of(literal_end.saturating_sub(1)),
    };
    let Some(connector) = connector else {
        return Some(Located::Connectors(property));
    };
    // Content between the quotes; `''` escapes stay put so offsets hold.
    let content_start = literal_start + 1;
    let content = &sql[content_start..literal_end.saturating_sub(1).max(content_start)];
    let object = connector_index(content, connector)
        .and_then(|index| array_object_spans(content).get(index).copied());
    Some(match object {
        Some((start, end)) => Located::Connector(LineSpan {
            first: line_of(content_start + start),
            last: line_of(content_start + end),
        }),
        None => Located::Connectors(property),
    })
}

/// A table or view declaration: its canonical name and source lines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementSpan {
    pub name: String,
    pub lines: LineSpan,
}

/// Every `CREATE TABLE|VIEW` statement in `sql`, in source order.
///
/// ```
/// use f5a::model::sql_locate::statement_spans;
///
/// let spans = statement_spans("CREATE TABLE t (x INT);\nCREATE VIEW V\nAS SELECT 1;");
/// assert_eq!(spans[1].name, "v", "bare names fold to lower case");
/// assert_eq!((spans[1].lines.first, spans[1].lines.last), (2, 3));
/// ```
pub fn statement_spans(sql: &str) -> Vec<StatementSpan> {
    let tokens = lex(sql);
    let line_of = |offset: usize| sql[..offset].matches('\n').count() + 1;
    (0..tokens.len())
        .filter_map(|index| declaration_at(&tokens, index))
        .filter_map(|statement| {
            let name = match &tokens[statement.name].kind {
                TokenKind::Word(word) => word.to_lowercase(),
                TokenKind::QuotedIdentifier(name) => name.clone(),
                _ => return None,
            };
            let last_token = tokens.get(statement.end).or(tokens.last())?;
            Some(StatementSpan {
                name,
                lines: LineSpan {
                    first: line_of(tokens[statement.create].start),
                    last: line_of(last_token.start),
                },
            })
        })
        .collect()
}

/// The table or view whose declaration spans `line`.
pub fn relation_at_line(statements: &[StatementSpan], line: usize) -> Option<&str> {
    statements
        .iter()
        .find(|statement| (statement.lines.first..=statement.lines.last).contains(&line))
        .map(|statement| statement.name.as_str())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TokenKind<'a> {
    Word(&'a str),
    QuotedIdentifier(String),
    /// A single-quoted string; the token spans both quotes.
    Literal,
    Symbol(char),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Token<'a> {
    kind: TokenKind<'a>,
    /// Byte offsets of the token in the source, end exclusive.
    start: usize,
    end: usize,
}

/// Split SQL into words, quoted identifiers, literals, and symbols,
/// dropping whitespace and comments.
fn lex(sql: &str) -> Vec<Token<'_>> {
    let is_word_character = |character: char| character.is_alphanumeric() || character == '_';
    let mut tokens = Vec::new();
    let mut offset = 0;
    while let Some(character) = sql[offset..].chars().next() {
        let start = offset;
        let rest = &sql[offset..];
        if character.is_whitespace() {
            offset += character.len_utf8();
            continue;
        }
        if rest.starts_with("--") {
            offset = rest.find('\n').map_or(sql.len(), |end| offset + end);
            continue;
        }
        if let Some(comment) = rest.strip_prefix("/*") {
            offset = comment
                .find("*/")
                .map_or(sql.len(), |end| offset + 2 + end + 2);
            continue;
        }
        let kind = if character == '\'' || character == '"' {
            offset = quoted_end(sql.as_bytes(), offset);
            if character == '\'' {
                TokenKind::Literal
            } else {
                let inner = &sql[start + 1..offset.saturating_sub(1).max(start + 1)];
                TokenKind::QuotedIdentifier(inner.replace("\"\"", "\""))
            }
        } else if is_word_character(character) {
            offset += rest
                .chars()
                .take_while(|next| is_word_character(*next))
                .map(char::len_utf8)
                .sum::<usize>();
            TokenKind::Word(&sql[start..offset])
        } else {
            offset += character.len_utf8();
            TokenKind::Symbol(character)
        };
        tokens.push(Token {
            kind,
            start,
            end: offset,
        });
    }
    tokens
}

/// End (exclusive) of the quoted token opening at `start`; a doubled quote
/// is an escape. An unterminated quote runs to the end of the text.
fn quoted_end(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let mut offset = start + 1;
    while offset < bytes.len() {
        if bytes[offset] == quote {
            if bytes.get(offset + 1) == Some(&quote) {
                offset += 2;
                continue;
            }
            return offset + 1;
        }
        offset += 1;
    }
    bytes.len()
}

/// Token indices of one `CREATE TABLE|VIEW` statement.
struct Statement {
    create: usize,
    name: usize,
    /// Index of the closing `;`, or the token count.
    end: usize,
}

impl Statement {
    fn body(&self) -> std::ops::Range<usize> {
        self.name + 1..self.end
    }
}

/// The statement declaring `relation`: an exact match on the canonical name
/// (bare identifiers fold to lower case) wins over a case-insensitive one.
fn find_statement(tokens: &[Token], relation: &str) -> Option<Statement> {
    let wanted = relation.trim_matches('"');
    let declarations: Vec<Statement> = (0..tokens.len())
        .filter_map(|index| declaration_at(tokens, index))
        .collect();
    let name_of = |statement: &Statement| match &tokens[statement.name].kind {
        TokenKind::Word(word) => Some(word.to_lowercase()),
        TokenKind::QuotedIdentifier(name) => Some(name.clone()),
        _ => None,
    };
    let exact = declarations
        .iter()
        .position(|statement| name_of(statement).as_deref() == Some(wanted));
    let index = exact.or_else(|| {
        declarations.iter().position(|statement| {
            name_of(statement).is_some_and(|name| name.eq_ignore_ascii_case(wanted))
        })
    })?;
    declarations.into_iter().nth(index)
}

/// Parse `CREATE [modifiers] TABLE|VIEW [IF NOT EXISTS] name` at `index`.
fn declaration_at(tokens: &[Token], index: usize) -> Option<Statement> {
    let is_word = |token: &Token, wanted: &str| matches!(token.kind, TokenKind::Word(word) if word.eq_ignore_ascii_case(wanted));
    if !is_word(&tokens[index], "create") {
        return None;
    }
    // Skip modifiers such as `OR REPLACE`, `MATERIALIZED`, or `LOCAL`.
    let mut cursor = index + 1;
    loop {
        let token = tokens.get(cursor)?;
        if is_word(token, "table") || is_word(token, "view") {
            break;
        }
        if !matches!(token.kind, TokenKind::Word(_)) || cursor > index + 4 {
            return None;
        }
        cursor += 1;
    }
    cursor += 1;
    if tokens.get(cursor).is_some_and(|token| is_word(token, "if")) {
        cursor += 3;
    }
    let name = cursor;
    if !matches!(
        tokens.get(name)?.kind,
        TokenKind::Word(_) | TokenKind::QuotedIdentifier(_)
    ) {
        return None;
    }
    let end = tokens[name..]
        .iter()
        .position(|token| token.kind == TokenKind::Symbol(';'))
        .map_or(tokens.len(), |offset| name + offset);
    Some(Statement {
        create: index,
        name,
        end,
    })
}

/// Byte span (quotes included) of the literal assigned to `'connectors'`.
fn connectors_literal(sql: &str, body: &[Token]) -> Option<(usize, usize)> {
    body.windows(3).find_map(|window| {
        let [key, equals, value] = window else {
            return None;
        };
        let is_connectors_key = key.kind == TokenKind::Literal
            && sql[key.start..key.end].eq_ignore_ascii_case("'connectors'");
        (is_connectors_key
            && equals.kind == TokenKind::Symbol('=')
            && value.kind == TokenKind::Literal)
            .then_some((value.start, value.end))
    })
}

/// Array position of the connector named `connector`; `unnamed-N` falls back
/// to position `N` when no element carries that name.
fn connector_index(content: &str, connector: &str) -> Option<usize> {
    let parsed: Value = serde_json::from_str(&content.replace("''", "'")).ok()?;
    let elements = parsed.as_array()?;
    let named = elements
        .iter()
        .position(|element| element.get("name").and_then(Value::as_str) == Some(connector));
    named.or_else(|| {
        let position: usize = connector.strip_prefix("unnamed-")?.parse().ok()?;
        elements
            .get(position)
            .filter(|element| element.get("name").is_none())
            .map(|_| position)
    })
}

/// Byte spans (`{` through `}`) of the objects directly inside the JSON
/// array `content`, skipping braces inside JSON strings.
fn array_object_spans(content: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut depth = 0usize;
    let mut object_start = 0;
    let mut is_in_string = false;
    let mut is_escaped = false;
    for (offset, character) in content.char_indices() {
        if is_in_string {
            match character {
                _ if is_escaped => is_escaped = false,
                '\\' => is_escaped = true,
                '"' => is_in_string = false,
                _ => {}
            }
            continue;
        }
        match character {
            '"' => is_in_string = true,
            '[' | '{' => {
                if depth == 1 && character == '{' {
                    object_start = offset;
                }
                depth += 1;
            }
            ']' | '}' => {
                depth = depth.saturating_sub(1);
                if depth == 1 && character == '}' {
                    spans.push((object_start, offset));
                }
            }
            _ => {}
        }
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::{LineSpan, Located, array_object_spans, locate};

    const PROGRAM: &str = r#"-- CREATE TABLE orders is mentioned in a comment first
CREATE TABLE orders (
    id INT,
    note VARCHAR -- it's fine
) WITH (
    'materialized' = 'true',
    'connectors' = '[
        {
            "name": "kafka_in",
            "transport": {"name": "kafka_input", "config": {"topic": "o''s {x}"}}
        },
        {
            "transport": {"name": "datagen"}
        }
    ]'
);

/* CREATE VIEW totals */
CREATE MATERIALIZED VIEW "Totals"
WITH ('connectors' = '[{"name": "delta_out"}]')
AS SELECT COUNT(*) FROM orders;

CREATE TABLE bare (x INT);
"#;

    fn span(first: usize, last: usize) -> LineSpan {
        LineSpan { first, last }
    }

    #[test]
    fn named_connectors_resolve_to_their_object() {
        assert_eq!(
            locate(PROGRAM, "orders", Some("kafka_in")),
            Some(Located::Connector(span(8, 11)))
        );
        assert_eq!(
            locate(PROGRAM, "Totals", Some("delta_out")),
            Some(Located::Connector(span(20, 20)))
        );
    }

    #[test]
    fn unnamed_connectors_resolve_by_position() {
        assert_eq!(
            locate(PROGRAM, "orders", Some("unnamed-1")),
            Some(Located::Connector(span(12, 14)))
        );
        assert_eq!(
            locate(PROGRAM, "orders", Some("unnamed-0")),
            Some(Located::Connectors(span(7, 15))),
            "position 0 has a name, so `unnamed-0` cannot be it"
        );
    }

    #[test]
    fn transport_names_never_impersonate_connector_names() {
        assert_eq!(
            locate(PROGRAM, "orders", Some("kafka_input")),
            Some(Located::Connectors(span(7, 15)))
        );
    }

    #[test]
    fn relations_without_a_connector_point_at_the_property_or_header() {
        assert_eq!(
            locate(PROGRAM, "orders", None),
            Some(Located::Connectors(span(7, 15)))
        );
        assert_eq!(
            locate(PROGRAM, "bare", Some("x")),
            Some(Located::Relation(span(23, 23)))
        );
        assert_eq!(locate(PROGRAM, "comment_only", None), None);
    }

    #[test]
    fn identifiers_match_canonically_then_case_insensitively() {
        assert!(locate(PROGRAM, "ORDERS", None).is_some(), "fallback match");
        assert!(
            locate(PROGRAM, "\"Totals\"", None).is_some(),
            "quotes stripped"
        );
        let both = "CREATE TABLE \"T\" (x INT);\nCREATE TABLE t (y INT);";
        assert_eq!(
            locate(both, "t", None),
            Some(Located::Relation(span(2, 2))),
            "bare t folds to lower case; the quoted \"T\" does not"
        );
        assert_eq!(locate(both, "T", None), Some(Located::Relation(span(1, 1))));
        let shouting = "CREATE TABLE \"ORDERS\" (x INT);\nCREATE TABLE Orders (y INT);";
        assert_eq!(
            locate(shouting, "orders", None),
            Some(Located::Relation(span(2, 2))),
            "bare Orders folds to orders before any case-insensitive guess"
        );
    }

    #[test]
    fn comments_and_literals_hide_lookalike_statements() {
        let sql = "-- CREATE TABLE ghost (x INT);\nSELECT 'CREATE TABLE ghost2';";
        assert_eq!(locate(sql, "ghost", None), None);
        assert_eq!(locate(sql, "ghost2", None), None);
    }

    #[test]
    fn broken_input_degrades_instead_of_panicking() {
        assert_eq!(locate("", "t", None), None);
        assert_eq!(locate("CREATE", "t", None), None);
        assert_eq!(locate("CREATE TABLE", "t", None), None);
        assert_eq!(
            locate(
                "CREATE TABLE t (x INT) WITH ('connectors' = '[{",
                "t",
                Some("a")
            ),
            Some(Located::Connectors(span(1, 1)))
        );
        assert_eq!(
            locate("CREATE TABLE IF NOT EXISTS t (x INT) /* open", "t", None),
            Some(Located::Relation(span(1, 1)))
        );
        assert_eq!(locate("CREATE FUNCTION t() RETURNS INT;", "t", None), None);
        assert_eq!(
            locate("CREATE TABLE ü (x INT);", "ü", None),
            Some(Located::Relation(span(1, 1)))
        );
    }

    #[test]
    fn statements_cover_their_lines_and_name_the_relation() {
        use super::{relation_at_line, statement_spans};
        let statements = statement_spans(PROGRAM);
        let names: Vec<&str> = statements.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["orders", "Totals", "bare"]);
        assert_eq!(relation_at_line(&statements, 9), Some("orders"));
        assert_eq!(relation_at_line(&statements, 21), Some("Totals"));
        assert_eq!(
            relation_at_line(&statements, 17),
            None,
            "between statements"
        );
        assert_eq!(relation_at_line(&statements, 99), None);
        let open = statement_spans("CREATE VIEW v AS\nSELECT 1");
        assert_eq!(
            open[0].lines.last, 2,
            "an unterminated statement runs to the end"
        );
    }

    #[test]
    fn object_spans_skip_braces_inside_json_strings() {
        let content = r#"[{"a": "}"}, {"b": "\"{"}, 3]"#;
        let spans = array_object_spans(content);
        assert_eq!(spans.len(), 2);
        assert_eq!(&content[spans[0].0..=spans[0].1], r#"{"a": "}"}"#);
        assert_eq!(&content[spans[1].0..=spans[1].1], r#"{"b": "\"{"}"#);
    }
}
