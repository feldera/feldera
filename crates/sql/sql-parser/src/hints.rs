//! Parse hint comments and attach them to their original SELECT/table nodes.
//!
//! Upstream skips comments while parsing. Original token endpoints identify the
//! only legal attachment sites; every hint must be consumed exactly once. This
//! keeps nested queries and aliases distinct without rewriting SQL tokens.

use sqlparser_064::{
    ast::{
        Ident, OptimizerHint, Select, Spanned, SqlHint, SqlHintKeyValue, SqlHintLiteral,
        SqlHintOptions, SqlHintValue, Statement, TableFactor, TableObject, Value, ValueWithSpan,
        VisitorMut,
    },
    dialect::Dialect,
    parser::{Parser, ParserError},
    tokenizer::{Location, Span, Token, TokenWithSpan, Whitespace},
};
use std::{collections::BTreeMap, ops::ControlFlow};

/// Hint bodies use Calcite's escaped-string token form without enabling it
/// for every expression in the prototype.
#[derive(Debug)]
struct HintDialect;

impl Dialect for HintDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        super::FelderaDialect.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        super::FelderaDialect.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }
    fn supports_unicode_string_literal(&self) -> bool {
        true
    }
    fn supports_string_escape_constant(&self) -> bool {
        true
    }
    fn preserves_unknown_string_escapes(&self) -> bool {
        true
    }
}

fn opener(comment: Span) -> Span {
    let start = comment.start;
    Span::new(start, Location::new(start.line, start.column + 3))
}

// Calcite uses lookahead at the start of each optional list element. When
// that element cannot start here it reports the delimiter that introduced it.
fn at_delimiter(error: ParserError, message: &str, delimiter: Span) -> ParserError {
    match error {
        ParserError::RecursionLimitExceeded | ParserError::Located { .. } => error,
        _ => super::diagnostic::located(message, delimiter),
    }
}

pub(super) struct Attachments {
    pending: BTreeMap<(u64, u64), OptimizerHint>,
}

impl Attachments {
    pub(super) fn parse(tokens: &[TokenWithSpan]) -> Result<Self, ParserError> {
        let mut pending = BTreeMap::new();
        let mut preceding_end = Location::new(0, 0);
        for token in tokens {
            if let Token::Whitespace(Whitespace::MultiLineComment(comment)) = &token.token {
                if let Some(text) = comment.strip_prefix('+') {
                    let hint = parse_comment(text, token.span)?;
                    let key = (preceding_end.line, preceding_end.column);
                    if pending.insert(key, hint).is_some() {
                        return Err(super::diagnostic::located(
                            "only one hint comment is allowed at each attachment site",
                            opener(token.span),
                        ));
                    }
                }
            } else if !matches!(token.token, Token::Whitespace(_)) {
                preceding_end = token.span.end;
            }
        }
        Ok(Self { pending })
    }

    fn take(&mut self, end: Location) -> Option<OptimizerHint> {
        self.pending.remove(&(end.line, end.column))
    }

    pub(super) fn finish(self) -> Result<(), ParserError> {
        if let Some(OptimizerHint::Structured { span, .. }) = self.pending.into_values().next() {
            return Err(super::diagnostic::located(
                "hint must follow SELECT or a named table reference",
                opener(span),
            ));
        }
        Ok(())
    }
}

impl VisitorMut for Attachments {
    type Break = ParserError;

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        if let Some(hint) = self.take(select.select_token.0.span.end) {
            select.optimizer_hints.push(hint);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name,
            args: None,
            optimizer_hints,
            ..
        } = table
        {
            if let Some(hint) = self.take(name.span().end) {
                optimizer_hints.push(hint);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        if let Statement::Insert(insert) = statement {
            if let TableObject::TableName(name) = &insert.table {
                if let Some(hint) = self.take(name.span().end) {
                    insert.table = TableObject::HintedTableName {
                        name: name.clone(),
                        hints: vec![hint],
                    };
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn parse_comment(text: &str, span: Span) -> Result<OptimizerHint, ParserError> {
    let mut tokens = super::unicode::tokens_with_dialect(text, &HintDialect)
        .map_err(|error| at_delimiter(error, "invalid hint comment", opener(span)))?;
    for token in &mut tokens {
        let shift = |location: Location| {
            Location::new(
                span.start.line + location.line - 1,
                location.column
                    + if location.line == 1 {
                        span.start.column + 2
                    } else {
                        0
                    },
            )
        };
        token.span = Span::new(shift(token.span.start), shift(token.span.end));
    }
    // The EOF is the original closing delimiter, not upstream's empty span.
    let end = Location::new(span.end.line, span.end.column - 2);
    tokens.push(TokenWithSpan {
        token: Token::EOF,
        span: Span::new(end, end),
    });
    let mut parser = Parser::new(&super::FelderaDialect).with_tokens_with_locations(tokens);
    let mut hints =
        vec![parse_hint(&mut parser)
            .map_err(|e| at_delimiter(e, "expected hint name", opener(span)))?];
    while parser.peek_token_ref().token == Token::Comma {
        let comma = parser.next_token().span;
        hints.push(
            parse_hint(&mut parser)
                .map_err(|e| at_delimiter(e, "expected hint after comma", comma))?,
        );
    }
    parser.expect_token(&Token::EOF)?;
    Ok(OptimizerHint::Structured { hints, span })
}

fn identifier(parser: &mut Parser) -> Result<Ident, ParserError> {
    let name = parser.parse_identifier()?;
    if let ControlFlow::Break(error) = super::reserved::identifier(&name) {
        return Err(error);
    }
    Ok(name)
}

fn single_literal(parser: &mut Parser) -> Result<ValueWithSpan, ParserError> {
    let start = parser.peek_token().span.start;
    let sign = if parser.consume_token(&Token::Minus) {
        Some('-')
    } else if parser.consume_token(&Token::Plus) {
        Some('+')
    } else {
        None
    };
    let mut value = parser.parse_value()?;
    if let Some(sign) = sign {
        let Value::Number(number, _) = &mut value.value else {
            return Err(super::diagnostic::at("hint signs require a number", start));
        };
        number.insert(0, sign);
        value.span.start = start;
    }
    if !matches!(
        value.value,
        Value::Number(..)
            | Value::SingleQuotedString(_)
            | Value::NationalStringLiteral(_)
            | Value::EscapedStringLiteral(_)
            | Value::UnicodeStringLiteral(_)
            | Value::HexStringLiteral(_)
    ) {
        return Err(super::diagnostic::at(
            "hint literals require strings or numbers",
            start,
        ));
    }
    Ok(value)
}

fn literal(parser: &mut Parser) -> Result<SqlHintLiteral, ParserError> {
    let first = single_literal(parser)?;
    if matches!(first.value, Value::Number(..)) {
        return Ok(SqlHintLiteral::Value(first));
    }
    let mut fragments = vec![first];
    while matches!(parser.peek_token().token, Token::SingleQuotedString(_)) {
        fragments.push(single_literal(parser)?);
    }
    Ok(if fragments.len() == 1 {
        SqlHintLiteral::Value(fragments.pop().expect("one literal"))
    } else {
        SqlHintLiteral::StringChain(fragments)
    })
}

fn value(parser: &mut Parser) -> Result<SqlHintValue, ParserError> {
    if matches!(parser.peek_token().token, Token::Word(_)) {
        identifier(parser).map(SqlHintValue::Identifier)
    } else {
        literal(parser).map(SqlHintValue::Literal)
    }
}

fn require_key(key: &SqlHintValue) -> Result<(), ParserError> {
    if let SqlHintValue::Literal(SqlHintLiteral::Value(value)) = key {
        if matches!(value.value, Value::Number(..)) {
            return Err(super::diagnostic::at(
                "hint keys require identifiers or strings",
                value.span.start,
            ));
        }
    }
    Ok(())
}

fn options(parser: &mut Parser) -> Result<SqlHintOptions, ParserError> {
    if parser.peek_token_ref().token != Token::LParen {
        return Ok(SqlHintOptions::Empty);
    }
    let opening = parser.next_token().span;
    if parser.consume_token(&Token::RParen) {
        return Ok(SqlHintOptions::Empty);
    }
    option_list(parser).map_err(|e| at_delimiter(e, "invalid hint options", opening))
}

fn following<T>(
    parser: &mut Parser,
    first: T,
    mut next: impl FnMut(&mut Parser) -> Result<T, ParserError>,
) -> Result<Vec<T>, ParserError> {
    let mut elements = vec![first];
    while parser.peek_token_ref().token == Token::Comma {
        let comma = parser.next_token().span;
        elements.push(
            next(parser).map_err(|e| at_delimiter(e, "invalid hint option after comma", comma))?,
        );
    }
    Ok(elements)
}

fn option_list(parser: &mut Parser) -> Result<SqlHintOptions, ParserError> {
    let first = value(parser)?;
    let options = if parser.consume_token(&Token::Eq) {
        require_key(&first)?;
        let first = SqlHintKeyValue {
            key: first,
            value: value(parser)?,
        };
        let pairs = following(parser, first, |parser| {
            let key = value(parser)?;
            require_key(&key)?;
            parser.expect_token(&Token::Eq)?;
            Ok(SqlHintKeyValue {
                key,
                value: value(parser)?,
            })
        })?;
        SqlHintOptions::KeyValue(pairs)
    } else {
        match first {
            SqlHintValue::Identifier(first) => {
                SqlHintOptions::Identifiers(following(parser, first, identifier)?)
            }
            SqlHintValue::Literal(first) => {
                SqlHintOptions::Literals(following(parser, first, literal)?)
            }
        }
    };
    parser.expect_token(&Token::RParen)?;
    Ok(options)
}

fn parse_hint(parser: &mut Parser) -> Result<SqlHint, ParserError> {
    let name = identifier(parser)?;
    let start = name.span.start;
    let options = options(parser)?;
    let end = parser.token_at(parser.index() - 1).span.end;
    Ok(SqlHint {
        name,
        options,
        span: Span::new(start, end),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser_064::ast::SetExpr;

    fn statements(sql: &str) -> Vec<super::super::statements::Statement> {
        super::super::statements::parse(super::super::unicode::tokens(sql).unwrap()).unwrap()
    }

    fn contents(hint: &OptimizerHint) -> &[SqlHint] {
        let OptimizerHint::Structured { hints, .. } = hint else {
            panic!("expected typed hint")
        };
        hints
    }

    #[test]
    fn string_chains_comments_and_escaped_values_keep_original_tokens() {
        let parsed =
            statements("SELECT /*+ h('a'\n'b'), escaped(E'abc'), commented(a/* inner */) */ 1");
        let super::super::statements::Statement::Upstream(statement) = &parsed[0] else {
            panic!()
        };
        let Statement::Query(query) = statement.as_ref() else {
            panic!()
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!()
        };
        let hints = contents(&select.optimizer_hints[0]);
        let SqlHintOptions::Literals(values) = &hints[0].options else {
            panic!()
        };
        let SqlHintLiteral::StringChain(fragments) = &values[0] else {
            panic!()
        };
        assert_eq!(fragments.len(), 2);
        assert_eq!(fragments[0].value, Value::SingleQuotedString("a".into()));
        assert_eq!(fragments[1].value, Value::SingleQuotedString("b".into()));
        assert_eq!(fragments[1].span.start, Location::new(2, 1));
        let SqlHintOptions::Literals(escaped) = &hints[1].options else {
            panic!()
        };
        assert!(matches!(&escaped[0], SqlHintLiteral::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(value), .. }) if value == "abc"));
        let SqlHintOptions::Identifiers(names) = &hints[2].options else {
            panic!()
        };
        assert_eq!(names[0].value, "a");
        assert!(super::super::parse("SELECT /* a /* b */ 1").is_ok());
        assert!(!super::super::parse("SELECT /* a /* b */ c */ 1").is_ok());
    }

    #[test]
    fn hint_options_keep_types_order_duplicates_quoting_and_original_spans() {
        let parsed = statements("SELECT 1;\nSELECT /*+ h(a='x', a='y'), g(\"T\") */ * FROM s.\"T\" /*+ scan(5, 'x') */ AS z");
        let super::super::statements::Statement::Upstream(statement) = &parsed[1] else {
            panic!()
        };
        let Statement::Query(query) = statement.as_ref() else {
            panic!()
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!()
        };
        let hints = contents(&select.optimizer_hints[0]);
        assert_eq!(hints.len(), 2);
        assert_eq!(hints[0].name.value, "h");
        assert_eq!(hints[0].span.start, Location::new(2, 12));
        let SqlHintOptions::KeyValue(pairs) = &hints[0].options else {
            panic!()
        };
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].key.to_string(), "a");
        assert_eq!(pairs[1].key.to_string(), "a");
        assert_eq!(pairs[0].value.to_string(), "'x'");
        assert_eq!(pairs[1].value.to_string(), "'y'");
        let SqlHintOptions::Identifiers(names) = &hints[1].options else {
            panic!()
        };
        assert_eq!(names[0].quote_style, Some('"'));
        let table = &select.from[0].relation;
        let TableFactor::Table {
            name,
            optimizer_hints,
            alias,
            ..
        } = table
        else {
            panic!()
        };
        assert_eq!(name.to_string(), "s.\"T\"");
        assert_eq!(alias.as_ref().unwrap().name.value, "z");
        let table_hints = contents(&optimizer_hints[0]);
        let SqlHintOptions::Literals(values) = &table_hints[0].options else {
            panic!()
        };
        assert!(
            matches!(&values[0], SqlHintLiteral::Value(ValueWithSpan { value: Value::Number(n, false), .. }) if n == "5")
        );
        assert!(
            matches!(&values[1], SqlHintLiteral::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) if s == "x")
        );
        assert!(table.span().end.column >= optimizer_hints[0].span().end.column);
        let rendered = statement.to_string();
        assert!(rendered.contains("SELECT /*+ h(a='x', a='y'), g(\"T\") */"));
        assert!(rendered.contains("s.\"T\" /*+ scan(5, 'x') */ AS z"));
        assert!(super::super::parse(&rendered).is_ok());
    }

    #[test]
    fn nested_query_hints_attach_to_their_own_nodes() {
        let parsed = statements("SELECT /*+ outer_hint */ * FROM (SELECT /*+ inner_hint */ * FROM t /*+ table_hint */) q");
        let super::super::statements::Statement::Upstream(statement) = &parsed[0] else {
            panic!()
        };
        let Statement::Query(query) = statement.as_ref() else {
            panic!()
        };
        let SetExpr::Select(outer) = query.body.as_ref() else {
            panic!()
        };
        assert_eq!(
            contents(&outer.optimizer_hints[0])[0].name.value,
            "outer_hint"
        );
        let TableFactor::Derived {
            subquery, alias, ..
        } = &outer.from[0].relation
        else {
            panic!()
        };
        assert_eq!(alias.as_ref().unwrap().name.value, "q");
        let SetExpr::Select(inner) = subquery.body.as_ref() else {
            panic!()
        };
        assert_eq!(
            contents(&inner.optimizer_hints[0])[0].name.value,
            "inner_hint"
        );
        let TableFactor::Table {
            optimizer_hints, ..
        } = &inner.from[0].relation
        else {
            panic!()
        };
        assert_eq!(contents(&optimizer_hints[0])[0].name.value, "table_hint");
    }

    #[test]
    fn table_hints_survive_dml_and_render_before_aliases_or_column_lists() {
        for sql in [
            "INSERT INTO t /*+ h */ (x) VALUES (1)",
            "UPDATE t /*+ h */ AS s SET x=1",
            "DELETE FROM t /*+ h */ AS s WHERE x=1",
            "MERGE INTO t /*+ h */ AS x USING s /*+ source_hint */ AS y ON x.a=y.a WHEN MATCHED THEN UPDATE SET a=y.a",
        ] {
            let parsed = statements(sql);
            let rendered = parsed[0].to_string();
            assert!(rendered.contains("t /*+ h */"), "{rendered}");
            assert!(super::super::parse(&rendered).is_ok(), "{rendered}");
        }
    }

    #[test]
    fn malformed_or_misplaced_hints_are_not_silently_discarded() {
        for sql in [
            "SELECT /*+ */ 1",
            "SELECT /*+ h(x)) */ 1",
            "SELECT /*+ h((x) */ 1",
            "SELECT /*+ h(')')) */ 1",
            "SELECT /*+ h(a, 1) */ 1",
            "SELECT /*+ h(1, a) */ 1",
            "SELECT /*+ h(a=1, 2=3) */ 1",
            "SELECT /*+ h(a=1, b) */ 1",
            "SELECT /*+ h(1+2) */ 1",
            "SELECT /*+ h */ /*+ g */ 1",
            "/*+ h */ SELECT 1",
            "SELECT 1 /*+ h */",
            "SELECT * FROM t AS x /*+ h */",
            "SELECT * FROM (SELECT 1) /*+ h */ q",
            "CREATE TABLE t /*+ h */ (x INT)",
            "INSERT /*+ h */ INTO t VALUES (1)",
            "SELECT * FROM f /*+ h */ (1)",
        ] {
            assert!(!super::super::parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SELECT /*+ h(')') */ 1",
            "SELECT /*+ h(\"(\") */ 1",
            "SELECT /* ordinary )) */ 1",
            "SELECT '/*+ h(x)) */'",
            "SELECT /* + h(x)) */ 1",
            "SELECT --+ h(x))\n1",
        ] {
            assert!(super::super::parse(sql).is_ok(), "{sql}");
        }
    }
}
