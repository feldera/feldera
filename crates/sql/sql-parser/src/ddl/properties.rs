//! WITH properties use Calcite's StringLiteral production on both sides of '='.
use sqlparser_064::{
    ast::{CreateTableOptions, Expr, Ident, Spanned, SqlOption, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(crate) fn parse(parser: &mut Parser) -> Result<CreateTableOptions, ParserError> {
    if !parser.parse_keyword(Keyword::WITH) {
        return Ok(CreateTableOptions::None);
    }
    if !parser.consume_token(&Token::LParen) {
        return Err(crate::diagnostic::located(
            "expected parenthesized properties after WITH",
            parser.get_current_token().span,
        ));
    }
    let options = parser.parse_comma_separated(|parser| {
        let key = literal(parser)?;
        parser.expect_token(&Token::Eq)?;
        let value = literal(parser)?;
        // Preserve the existing representation for ordinary single-quoted keys.
        if let Expr::Value(lit) = &key {
            if let Value::SingleQuotedString(name) = &lit.value {
                return Ok(SqlOption::KeyValue {
                    key: Ident::with_quote_and_span('\'', lit.span, name).into(),
                    value,
                });
            }
        }
        Ok(SqlOption::LiteralKeyValue {
            key: Box::new(key),
            value,
        })
    })?;
    parser.expect_token(&Token::RParen)?;
    Ok(CreateTableOptions::With(options))
}

pub(crate) fn is_string(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(lit) if matches!(lit.value, Value::SingleQuotedString(_)
        | Value::NationalStringLiteral(_) | Value::EscapedStringLiteral(_)
        | Value::HexStringLiteral(_) | Value::UnicodeStringLiteral(_)))
        || matches!(expr, Expr::StringLiteralChain(_) | Expr::Prefixed { .. })
}

fn literal(parser: &mut Parser) -> Result<Expr, ParserError> {
    let first = parser.peek_token_ref();
    let string = match &first.token {
        Token::SingleQuotedString(_)
        | Token::NationalStringLiteral(_)
        | Token::EscapedStringLiteral(_)
        | Token::HexStringLiteral(_)
        | Token::UnicodeStringLiteral(_) => true,
        Token::Word(word) if word.quote_style.is_none() && word.value.starts_with('_') => {
            let next = parser.peek_nth_token_ref(1);
            matches!(next.token, Token::SingleQuotedString(_)) && first.span.end == next.span.start
        }
        _ => false,
    };
    if !string {
        return parser.expected_ref("string literal", first);
    }
    let mut head = parser.parse_prefix()?;
    if matches!(parser.peek_token_ref().token, Token::SingleQuotedString(_)) {
        match &mut head {
            Expr::Prefixed { value, .. } => **value = continued(parser, value, false)?,
            Expr::Value(v)
                if matches!(
                    v.value,
                    Value::NationalStringLiteral(_) | Value::HexStringLiteral(_)
                ) =>
            {
                let binary = matches!(v.value, Value::HexStringLiteral(_));
                head = continued(parser, &head, binary)?;
            }
            _ => (),
        }
    }
    Ok(head)
}

fn continued(parser: &mut Parser, head: &Expr, binary: bool) -> Result<Expr, ParserError> {
    let Expr::Value(value) = head else {
        return Err(crate::diagnostic::located(
            "expected a string literal fragment",
            head.span(),
        ));
    };
    let mut fragments = vec![value.clone()];
    while let Token::SingleQuotedString(text) = &parser.peek_token_ref().token {
        if binary
            && !text
                .trim_matches('\'')
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(crate::diagnostic::located(
                "invalid hexadecimal literal",
                parser.peek_token_ref().span,
            ));
        }
        fragments.push(parser.parse_value()?);
    }
    Ok(Expr::StringLiteralChain(fragments))
}
