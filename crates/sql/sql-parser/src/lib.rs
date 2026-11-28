//! Feldera SQL syntax and source locations.
//!
//! The returned statements preserve Feldera-specific declarations and the
//! upstream typed syntax for ordinary SQL. Catalog resolution belongs to the binder.

#[cfg(test)]
mod aggregate_tests;
mod ast_fields;
mod attributes;
mod collections;
mod create_type;
pub use create_type::{Attribute, CreateType, TypeDefinition};
mod datetime_difference;
mod ddl;
mod diagnostic;
pub use diagnostic::{syntax_diagnostic, SyntaxDiagnostic};
mod expression_policy;
mod functions;
mod grammar;
mod hints;
mod json_object;
mod lateness;
mod reject;
mod relations;
pub(crate) mod reserved;
mod settings;
mod statements;
mod unparse;
pub use unparse::emit_statements;
mod string_literals;
mod temporal_types;
mod translate;
mod trim;
mod type_policy;
mod unicode;
mod values;

pub use statements::{ExplainDepth, ExplainDetail, ExplainFormat, ExplainPlan, Statement, ViewColumn};

#[derive(Debug)]
pub struct FelderaDialect;

/// Reserved identifiers in the Feldera grammar.
pub fn reserved_keywords() -> &'static [&'static str] {
    reserved::RESERVED
}
use sqlparser_064::{
    ast::{
        ArrayElemTypeDef, ColumnOption, DataType, Expr, Statement as UpstreamStatement,
        StructBracketKind, StructField, Value, ValueWithSpan,
    },
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan},
};

impl Dialect for FelderaDialect {
    fn supports_qualified_wildcard_operand(&self) -> bool {
        true
    }

    fn supports_format_json(&self) -> bool {
        true
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        // Calcite requires IS in the postfix null test. An adjacent NOT NULL
        // can instead be a column or type modifier for the enclosing parser.
        if parser.peek_keyword(Keyword::NOT)
            && matches!(&parser.peek_nth_token_ref(1).token, Token::Word(w) if w.keyword == Keyword::NULL)
        {
            return Some(Ok(self.prec_unknown()));
        }
        if parser.peek_token_ref().token == Token::Colon {
            // The pinned dialect uses a colon between JSON object keys and values.
            return Some(Ok(self.prec_unknown()));
        }
        if parser.peek_keyword(Keyword::FORMAT)
            && matches!(&parser.peek_nth_token_ref(1).token, Token::Word(w) if w.keyword == Keyword::JSON)
        {
            // FORMAT JSON binds below comparisons and above prefix NOT.
            return Some(Ok(self.prec_value(sqlparser_064::dialect::Precedence::Is)));
        }
        None
    }

    fn supports_multiset_membership(&self) -> bool {
        true
    }

    fn supports_window_frame_exclusion(&self) -> bool {
        true
    }

    fn supports_system_time_as_of(&self) -> bool {
        true
    }

    fn is_select_item_alias(&self, explicit: bool, keyword: &Keyword, parser: &mut Parser) -> bool {
        explicit || *keyword == Keyword::END || self.is_column_alias(keyword, parser)
    }

    fn supports_local_time_zone(&self) -> bool {
        true
    }

    fn supports_string_escape_constant(&self) -> bool {
        true
    }

    fn preserves_unknown_string_escapes(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        false
    }

    fn parenthesized_suffix(
        &self,
        parser: &mut Parser,
        expr: Expr,
        opening_index: usize,
    ) -> Result<Expr, ParserError> {
        datetime_difference::parse(parser, expr, opening_index)
    }

    fn supports_slash_line_comments(&self) -> bool {
        true
    }

    fn supports_sql_hint_comments(&self) -> bool {
        true
    }

    fn parse_values(
        &self,
        parser: &mut Parser,
        _allow_empty: bool,
        value_keyword: bool,
    ) -> Option<Result<sqlparser_064::ast::Values, ParserError>> {
        Some(values::parse(parser, value_keyword))
    }

    fn parse_join(
        &self,
        parser: &mut Parser,
    ) -> Option<Result<sqlparser_064::ast::Join, ParserError>> {
        relations::join(parser)
    }

    fn parse_explicit_table_name(
        &self,
        parser: &mut Parser,
    ) -> Option<Result<sqlparser_064::ast::ObjectName, ParserError>> {
        Some(statements::compound_name(parser))
    }

    fn parse_function_arg(
        &self,
        parser: &mut Parser,
    ) -> Option<Result<sqlparser_064::ast::FunctionArg, ParserError>> {
        relations::function_argument(parser)
    }

    fn supports_repeated_array_suffix(&self) -> bool {
        true
    }

    fn supports_array_suffix_cardinality(&self) -> bool {
        false
    }

    fn parse_statement(
        &self,
        parser: &mut Parser,
    ) -> Option<Result<UpstreamStatement, ParserError>> {
        ddl::declaration::parse(parser)
            .or_else(|| functions::parse(parser))
            .or_else(|| settings::parse(parser))
    }

    fn parse_column_option(
        &self,
        parser: &mut Parser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        let start = parser.peek_token().span;
        if parser.parse_keywords(&[Keyword::FOREIGN, Keyword::KEY, Keyword::REFERENCES]) {
            return Ok(Some(ddl::inline_foreign_key(parser, start).map(Some)));
        }
        Ok(lateness::parse_option(parser)?.map(|option| Ok(Some(option))))
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || matches!(ch, '_' | '$')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }
    fn supports_lambda_functions(&self) -> bool {
        true
    }
    fn supports_numeric_prefix(&self) -> bool {
        true
    }
    fn supports_projection_trailing_commas(&self) -> bool {
        true
    }
    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }
    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }

    fn supports_map_literal_with_angle_brackets(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    fn supports_limit_comma_all(&self) -> bool {
        true
    }

    fn supports_wildcard_except_as_exclude(&self) -> bool {
        true
    }

    fn supports_grouped_unpivot(&self) -> bool {
        true
    }

    fn supports_nested_grouping_sets(&self) -> bool {
        true
    }

    fn supports_group_by_distinct(&self) -> bool {
        true
    }

    fn supports_between_symmetry(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_inline_null_treatment_for(
        &self,
        name: &[sqlparser_064::ast::ObjectNamePart],
    ) -> bool {
        self.supports_function_separator(name) || grammar::is_percentile(name)
    }

    fn supports_function_separator(&self, name: &[sqlparser_064::ast::ObjectNamePart]) -> bool {
        matches!(name, [sqlparser_064::ast::ObjectNamePart::Identifier(ident)] if ident.quote_style.is_none()
            && matches!(ident.value.to_ascii_uppercase().as_str(), "ARRAY_AGG" | "ARRAY_CONCAT_AGG" | "STRING_AGG" | "GROUP_CONCAT"))
    }

    fn supports_string_literal_table_alias(&self, _explicit: bool) -> bool {
        false
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        if matches!(&parser.peek_token_ref().token, Token::Word(w)
            if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("JSON_OBJECT"))
            && parser.peek_nth_token_ref(1).token == Token::LParen
        {
            return Some(json_object::parse(parser));
        }
        if parser.peek_keyword(Keyword::UNIQUE) {
            let start = parser.next_token().span;
            // Calcite places UNIQUE at the additive level and includes a
            // following addition in its operand, before comparison predicates.
            let precedence = self.prec_value(sqlparser_064::dialect::Precedence::PlusMinus) - 1;
            return Some(parser.parse_subexpr(precedence).map(|expr| Expr::Unique {
                expr: Box::new(expr),
                span: start.union(&parser.get_current_token().span),
            }));
        }
        if parser.peek_keyword(Keyword::TRANSLATE)
            && parser.peek_nth_token(1).token == Token::LParen
        {
            return Some(translate::parse(parser));
        }
        if parser.peek_keyword(Keyword::EXTRACT) && parser.peek_nth_token(1).token == Token::LParen
        {
            return Some(temporal_types::parse_extract(parser));
        }
        if [Keyword::CAST, Keyword::TRY_CAST, Keyword::SAFE_CAST]
            .iter()
            .any(|keyword| parser.peek_keyword(*keyword))
            && parser.peek_nth_token(1).token == Token::LParen
        {
            return Some(temporal_types::parse_cast(parser));
        }
        if parser.peek_keyword(Keyword::TRIM) && parser.peek_nth_token(1).token == Token::LParen {
            return Some(trim::parse(parser));
        }
        if parser.peek_keyword(Keyword::ROW) && parser.peek_nth_token(1).token == Token::LParen {
            return Some(collections::parse_row(parser));
        }
        if parser.peek_keyword(Keyword::MULTISET)
            && matches!(parser.peek_nth_token_ref(1).token, Token::LParen | Token::LBracket)
        {
            return Some(collections::parse_multiset(parser));
        }
        if parser.peek_keyword(Keyword::MAP)
            && parser.peek_nth_token(1).token == Token::LParen
            && matches!(parser.peek_nth_token(2).token, Token::Word(w) if matches!(w.keyword, Keyword::SELECT | Keyword::WITH | Keyword::VALUES | Keyword::TABLE))
        {
            let start = parser.next_token().span;
            parser.next_token();
            return Some(parser.parse_query().and_then(|query| {
                let end = parser.peek_token().span;
                parser.expect_token(&Token::RParen)?;
                Ok(Expr::MapQuery {
                    query,
                    span: start.union(&end),
                })
            }));
        }
        if parser.peek_keyword(Keyword::MAP) && parser.peek_nth_token(1).token == Token::LBracket {
            return Some(collections::parse_map(parser));
        }
        if !matches!(
            parser.peek_token().token,
            Token::SingleQuotedString(_) | Token::UnicodeStringLiteral(_)
        ) {
            return None;
        }
        Some(parse_string_fragments(parser))
    }

    fn validate_data_type(
        &self,
        ty: &DataType,
        tokens: &[TokenWithSpan],
    ) -> Result<(), ParserError> {
        diagnostic::validate_data_type(ty, tokens)
    }

    fn parse_data_type(&self, parser: &mut Parser) -> Option<Result<DataType, ParserError>> {
        if let Some(error) = type_policy::invalid_parameters(parser) {
            return Some(Err(error));
        }
        let start = parser.peek_token().span.start;
        if parser.parse_keyword(Keyword::INTERVAL) {
            return Some(Err(diagnostic::at(
                "interval type qualifiers are supported only as a direct CAST target",
                start,
            )));
        }
        if !parser.parse_keyword(Keyword::ROW) {
            return None;
        }
        Some(parse_row_type(parser))
    }
}

fn parse_string_fragments(parser: &mut Parser) -> Result<Expr, ParserError> {
    let first = parser.next_token();
    let value = match first.token {
        Token::SingleQuotedString(text) => Value::SingleQuotedString(text),
        Token::UnicodeStringLiteral(text) => Value::UnicodeStringLiteral(text),
        _ => unreachable!("prefix hook checked the first token"),
    };
    let mut fragments = vec![ValueWithSpan {
        value,
        span: first.span,
    }];
    while let Token::SingleQuotedString(fragment) = parser.peek_token().token {
        let next = parser.next_token();
        fragments.push(ValueWithSpan {
            value: Value::SingleQuotedString(fragment),
            span: next.span,
        });
    }
    Ok(if fragments.len() == 1 {
        Expr::Value(fragments.pop().expect("one fragment"))
    } else {
        Expr::StringLiteralChain(fragments)
    })
}

fn parse_row_type(parser: &mut Parser) -> Result<DataType, ParserError> {
    parser.expect_token(&Token::LParen)?;
    let fields = parser.parse_comma_separated(|parser| {
        let name = parser.parse_identifier()?;
        if name.quote_style.is_some_and(|quote| quote != '"') {
            return Err(diagnostic::at(
                "ROW field names require identifiers or double quotes",
                name.span.start,
            ));
        }
        let field_type = parser.parse_data_type()?;
        let nullable = if parser.parse_keyword(Keyword::NOT) {
            parser.expect_keyword(Keyword::NULL)?;
            Some(false)
        } else {
            parser.parse_keyword(Keyword::NULL).then_some(true)
        };
        Ok(StructField {
            field_name: Some(name),
            nullable,
            field_type,
            options: None,
        })
    })?;
    parser.expect_token(&Token::RParen)?;
    array_suffixes(
        parser,
        DataType::Struct(fields, StructBracketKind::Parentheses),
    )
}

fn array_suffixes(parser: &mut Parser, mut data_type: DataType) -> Result<DataType, ParserError> {
    // The hook bypasses upstream suffix parsing. Feldera permits any number
    // of plain ARRAY layers, but no bracketed cardinalities.
    while parser.parse_keyword(Keyword::ARRAY) {
        data_type = DataType::Array(ArrayElemTypeDef::Qualified(Box::new(data_type), None));
    }
    Ok(data_type)
}

/// Tokenize the original SQL using the same Unicode escapes and source spans
/// as the statement parser. Binder diagnostics use these source coordinates.
pub fn source_tokens(sql: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
    unicode::tokens(sql)
}

/// Parse a complete SQL source file.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    parse_at(sql, 0)
}

/// Parse an API submission after the given number of prior source lines.
pub fn parse_at(sql: &str, preceding_lines: u64) -> Result<Vec<Statement>, ParserError> {
    parse_at_with_starts(sql, preceding_lines, |_| {})
}

/// Parse a submission and report the first token of each complete statement.
/// Coordinates include the preceding submissions, including when they omit a semicolon.
pub fn parse_at_with_starts(
    sql: &str,
    preceding_lines: u64,
    record_start: impl FnMut(sqlparser_064::tokenizer::Location),
) -> Result<Vec<Statement>, ParserError> {
    let mut unicode_names = Vec::new();
    let mut tokens = unicode::tokenize(sql, &FelderaDialect, |pos| unicode_names.push(pos))?;
    if preceding_lines != 0 {
        for token in &mut tokens {
            if token.span.start.line != 0 {
                token.span.start.line = token
                    .span
                    .start
                    .line
                    .checked_add(preceding_lines)
                    .ok_or_else(|| {
                        ParserError::ParserError(
                            "SQL source line exceeds the supported range".into(),
                        )
                    })?;
            }
            if token.span.end.line != 0 {
                token.span.end.line = token
                    .span
                    .end
                    .line
                    .checked_add(preceding_lines)
                    .ok_or_else(|| {
                        ParserError::ParserError(
                            "SQL source line exceeds the supported range".into(),
                        )
                    })?;
            }
        }
    }
    reserved::validate_tokens(&tokens)?;
    reject::validate_lossy_tokens(&tokens)?;
    attributes::validate_tokens(&mut tokens)?;
    // Calcite distinguishes ordinary quoted names from U&"..." in the
    // DESCRIBE lookahead. Both have the same decoded identifier token.
    // Each location also belongs to a checked token span above.
    for name in &mut unicode_names {
        name.line += preceding_lines;
    }
    statements::parse_with_starts(tokens, &unicode_names, record_start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser_064::ast::{DataType, Statement};

    #[test]
    fn row_type_retains_named_fields_and_nested_structure() {
        let statements = Parser::parse_sql(
            &FelderaDialect,
            "CREATE TABLE t (r ROW(a INTEGER, \"B\" ROW(c VARCHAR)))",
        )
        .unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected CREATE TABLE")
        };
        let DataType::Struct(fields, _) = &table.columns[0].data_type else {
            panic!(
                "ROW lost its typed fields: {:?}",
                table.columns[0].data_type
            )
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field_name.as_ref().unwrap().value, "a");
        assert!(matches!(fields[0].field_type, DataType::Integer(_)));
        assert_eq!(
            fields[1].field_name.as_ref().unwrap().quote_style,
            Some('"')
        );
        let DataType::Struct(nested, _) = &fields[1].field_type else {
            panic!("nested ROW lost its fields")
        };
        assert_eq!(nested[0].field_name.as_ref().unwrap().value, "c");
        assert!(matches!(nested[0].field_type, DataType::Varchar(_)));
    }

    #[test]
    fn row_array_suffixes_preserve_each_layer_and_reject_cardinality() {
        for (suffix, layers) in [("ARRAY", 1), ("ARRAY ARRAY", 2), ("ARRAY ARRAY ARRAY", 3)] {
            let sql = format!("CREATE TABLE t (r ROW(a INTEGER) {suffix})");
            let statements = Parser::parse_sql(&FelderaDialect, &sql).unwrap();
            let Statement::CreateTable(table) = &statements[0] else {
                panic!("expected CREATE TABLE")
            };
            let mut ty = &table.columns[0].data_type;
            for _ in 0..layers {
                let DataType::Array(ArrayElemTypeDef::Qualified(element, None)) = ty else {
                    panic!("array layer missing")
                };
                ty = element;
            }
            assert!(matches!(ty, DataType::Struct(_, _)));
        }
        // The previous probe accepted these upstream forms. The pinned Java
        // grammar rejects them; keep that negative evidence in the audit.
        for suffix in ["ARRAY[3]", "ARRAY[]", "[]", "[3]"] {
            let sql = format!("CREATE TABLE t (r ROW(a INTEGER) {suffix})");
            assert!(!parse(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn malformed_rows_are_rejected_without_reinterpreting_fields() {
        for body in [
            "ROW(a)",
            "ROW(a INT,)",
            "ROW('a' INT)",
            "ROW(a INT NOT NULL NULL)",
            "ROW(a INT) ARRAY[-1]",
        ] {
            let sql = format!("CREATE TABLE t (r {body})");
            assert!(Parser::parse_sql(&FelderaDialect, &sql).is_err(), "{sql}");
        }
        // This hook only runs in type position; constructors remain expressions.
        assert!(Parser::parse_sql(&FelderaDialect, "SELECT ROW(a, b) FROM t").is_ok());
        let statements = Parser::parse_sql(&FelderaDialect, "CREATE TABLE t (r \"ROW\")").unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected CREATE TABLE")
        };
        assert!(matches!(table.columns[0].data_type, DataType::Custom(_, _)));
    }
    #[test]
    fn row_field_nullability_is_explicit_in_the_ast() {
        let statements = Parser::parse_sql(
            &FelderaDialect,
            "CREATE TABLE t (r ROW(a INT NULL, b ROW(c VARCHAR NOT NULL) NULL))",
        )
        .unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected CREATE TABLE")
        };
        let DataType::Struct(fields, _) = &table.columns[0].data_type else {
            panic!("expected ROW")
        };
        assert_eq!(fields[0].nullable, Some(true));
        assert_eq!(fields[1].nullable, Some(true));
        let DataType::Struct(fields, _) = &fields[1].field_type else {
            panic!("nested ROW lost")
        };
        assert_eq!(fields[0].nullable, Some(false));
    }
    #[test]
    fn adjacent_strings_keep_values_spans_and_explicit_aliases() {
        use sqlparser_064::ast::{SelectItem, SetExpr, Value};
        for separator in ["\n", "\r\n", " /*\n*/ ", " -- comment\n"] {
            let sql = format!("SELECT 'a'{separator}'b' AS x");
            let statements = Parser::parse_sql(&FelderaDialect, &sql).unwrap();
            let Statement::Query(query) = &statements[0] else {
                panic!("expected query")
            };
            let SetExpr::Select(select) = query.body.as_ref() else {
                panic!("expected select")
            };
            let SelectItem::ExprWithAlias {
                expr: Expr::StringLiteralChain(values),
                alias,
            } = &select.projection[0]
            else {
                panic!("fragments became an alias")
            };
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].value, Value::SingleQuotedString("a".into()));
            assert_eq!(values[1].value, Value::SingleQuotedString("b".into()));
            assert_eq!(values[0].span.start.line, 1);
            assert_eq!(values[1].span.end.line, 2);
            assert_eq!(alias.value, "x");
        }
        for (sql, formatted) in [
            ("SELECT 'a' /*same line*/ 'b'", "SELECT 'a' 'b'"),
            ("SELECT 'a' 'b'\n'c'", "SELECT 'a' 'b'\n'c'"),
        ] {
            let statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
            assert_eq!(statements[0].to_string(), formatted);
        }
        assert!(Parser::parse_sql(&FelderaDialect, "SELECT 'a' AS 'b'").is_ok());
        assert!(Parser::parse_sql(&FelderaDialect, "SELECT 1 'x'").is_ok());
        for sql in ["SELECT * FROM t AS 'x'", "SELECT * FROM t 'x'"] {
            assert!(Parser::parse_sql(&FelderaDialect, sql).is_err());
        }
    }
}
