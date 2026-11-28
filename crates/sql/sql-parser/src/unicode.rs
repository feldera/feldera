//! Decode Unicode identifiers and literal fragments after reading their escape clause.
//!
//! Token adjacency distinguishes `U&"name"` from a bitwise expression. Keep
//! original source spans; decoded character counts cannot reconstruct them.

use super::FelderaDialect;
use sqlparser_064::{
    dialect::Dialect,
    keywords::Keyword,
    parser::ParserError,
    tokenizer::{Location, Token, TokenWithSpan, Tokenizer},
};

pub(super) fn tokens(sql: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
    tokens_with_dialect(sql, &FelderaDialect)
}

pub(super) fn tokens_with_dialect(
    sql: &str,
    dialect: &dyn Dialect,
) -> Result<Vec<TokenWithSpan>, ParserError> {
    tokenize(sql, dialect, |_| {})
}

pub(super) fn tokenize(
    sql: &str,
    dialect: &dyn Dialect,
    mut record_unicode_name: impl FnMut(Location),
) -> Result<Vec<TokenWithSpan>, ParserError> {
    let tokens = super::string_literals::join_charset_prefixes(
        Tokenizer::new(dialect, sql)
            .with_raw_unicode_literals()
            .tokenize_with_location()?,
    );
    let mut result = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while index < tokens.len() {
        if matches!(tokens[index].token, Token::UnicodeStringLiteral(_)) {
            index = unicode_literal(&tokens, index, &mut result)?;
            continue;
        }
        let Some(name) = unicode_name(&tokens[index..]) else {
            result.push(tokens[index].clone());
            index += 1;
            continue;
        };
        let (escape, end) = escape_clause(&tokens, index + 3)?;
        let mut token = tokens[index].clone();
        let name = decode(name, escape).map_err(|error| match error {
            ParserError::ParserError(message) => super::diagnostic::at(message, token.span.start),
            error => error,
        })?;
        record_unicode_name(token.span.start);
        token.token = Token::make_word(&name, Some('"'));
        token.span.end = tokens[end - 1].span.end;
        result.push(token);
        index = end;
    }
    Ok(result)
}

fn escape_clause(tokens: &[TokenWithSpan], end: usize) -> Result<(char, usize), ParserError> {
    let suffix = skip_whitespace(tokens, end);
    if !matches!(tokens.get(suffix).map(|t| &t.token), Some(Token::Word(w)) if w.keyword == Keyword::UESCAPE)
    {
        return Ok(('\\', end));
    }
    let index = skip_whitespace(tokens, suffix + 1);
    let Some(TokenWithSpan {
        token: Token::SingleQuotedString(text),
        span,
    }) = tokens.get(index)
    else {
        return Err(super::diagnostic::at(
            "UESCAPE requires a single-character string",
            tokens[suffix].span.start,
        ));
    };
    let mut chars = text.chars();
    match chars.next() {
        Some(ch) if chars.next().is_none() && valid_escape(ch) => Ok((ch, index + 1)),
        _ => Err(super::diagnostic::at(
            "unsupported or invalid UESCAPE character",
            span.start,
        )),
    }
}

fn unicode_literal(
    tokens: &[TokenWithSpan],
    start: usize,
    result: &mut Vec<TokenWithSpan>,
) -> Result<usize, ParserError> {
    let mut end = start + 1;
    loop {
        let next = skip_whitespace(tokens, end);
        if !matches!(
            tokens.get(next).map(|t| &t.token),
            Some(Token::SingleQuotedString(_))
        ) {
            break;
        }
        end = next + 1;
    }
    let (escape, next) = escape_clause(tokens, end)?;
    for raw in &tokens[start..end] {
        let mut token = raw.clone();
        if let Token::UnicodeStringLiteral(text) | Token::SingleQuotedString(text) =
            &mut token.token
        {
            *text = decode(text, escape)
                .map_err(|error| super::diagnostic::at(error.to_string(), raw.span.start))?;
        }
        result.push(token);
    }
    if next > end {
        result
            .last_mut()
            .expect("at least the first fragment")
            .span
            .end = tokens[next - 1].span.end;
    }
    Ok(next)
}

fn unicode_name(tokens: &[TokenWithSpan]) -> Option<&str> {
    let [prefix, ampersand, quoted, ..] = tokens else {
        return None;
    };
    match (&prefix.token, &ampersand.token, &quoted.token) {
        (Token::Word(prefix_word), Token::Ampersand, Token::Word(name))
            if prefix_word.quote_style.is_none()
                && prefix_word.value.eq_ignore_ascii_case("u")
                && name.quote_style == Some('"')
                && prefix.span.end == ampersand.span.start
                && ampersand.span.end == quoted.span.start =>
        {
            Some(&name.value)
        }
        _ => None,
    }
}

fn skip_whitespace(tokens: &[TokenWithSpan], mut index: usize) -> usize {
    while matches!(
        tokens.get(index).map(|t| &t.token),
        Some(Token::Whitespace(_))
    ) {
        index += 1;
    }
    index
}

fn valid_escape(ch: char) -> bool {
    // Restrict the prototype to ASCII escape characters. Java's character
    // classification and one-UTF-16-unit requirement need a separate audit.
    ch.is_ascii()
        && !ch.is_ascii_hexdigit()
        && !ch.is_ascii_whitespace()
        && !matches!(ch, '+' | '"')
}

fn decode(text: &str, escape: char) -> Result<String, ParserError> {
    let mut units = Vec::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != escape {
            units.extend_from_slice(ch.encode_utf16(&mut [0; 2]));
            continue;
        }
        let first = chars
            .next()
            .ok_or_else(|| error("incomplete Unicode escape"))?;
        if first == escape {
            units.push(escape as u16);
            continue;
        }
        // Calcite reads one UTF-16 code unit with Integer.parseInt on a
        // four-character field. A sign occupies the first character.
        let signed = matches!(first, '+' | '-');
        let mut value = if signed {
            0
        } else {
            first
                .to_digit(16)
                .ok_or_else(|| error("invalid Unicode escape"))? as i32
        };
        for _ in 0..3 {
            let digit = chars
                .next()
                .and_then(|c| c.to_digit(16))
                .ok_or_else(|| error("Unicode escape requires four hexadecimal digits"))?;
            value = value * 16 + digit as i32;
        }
        if signed && first == '-' {
            value = -value;
        }
        units.push(value as u16);
    }
    String::from_utf16(&units).map_err(|_| error("unpaired surrogate in Unicode escape"))
}

fn error(message: &str) -> ParserError {
    ParserError::ParserError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser_064::{
        ast::{Expr, SelectItem, SetExpr, Statement},
        parser::Parser,
    };

    fn parse(sql: &str) -> Vec<Statement> {
        Parser::new(&FelderaDialect)
            .with_tokens_with_locations(tokens(sql).unwrap())
            .parse_statements()
            .unwrap()
    }

    #[test]
    fn identifiers_decode_in_expression_table_and_column_contexts() {
        let statements = parse(r#"CREATE TABLE U&"t\0062" (U&"x!0061" UESCAPE '!' INT)"#);
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected table")
        };
        assert_eq!(table.name.to_string(), "\"tb\"");
        assert_eq!(table.columns[0].name.value, "xa");
        let statements = parse(r#"SELECT t.U&"d\0061t" FROM t"#);
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select")
        };
        let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) = &select.projection[0] else {
            panic!("identifier became a bitwise expression")
        };
        assert_eq!(parts[1].value, "dat");
        assert_eq!(parts[1].span.start.column, 10);
        assert_eq!(parts[1].span.end.column, 21);
    }

    #[test]
    fn escapes_are_strict_and_surrogate_pairs_are_preserved() {
        assert_eq!(decode(r"\D83D\DE00", '\\').unwrap(), "😀");
        assert_eq!(decode(r"x\\y", '\\').unwrap(), r"x\y");
        assert_eq!(decode(r"\0000", '\\').unwrap(), "\0");
        assert_eq!(decode(r"\+01F600", '\\').unwrap(), "\u{1f}600");
        assert_eq!(decode(r"\-001", '\\').unwrap(), "\u{ffff}");
        for text in [r"\D800", r"\DE00", r"\12", r"\zzzz", r"\+0x1"] {
            assert!(decode(text, '\\').is_err(), "{text}");
        }
        for suffix in ["UESCAPE ''", "UESCAPE 'ab'", "UESCAPE '+'", "UESCAPE 'A'"] {
            assert!(tokens(&format!(r#"SELECT U&"x" {suffix}"#)).is_err());
        }
    }

    #[test]
    fn whitespace_and_comments_prevent_prefix_reinterpretation() {
        for sql in [r#"SELECT U & "d\0061t""#, r#"SELECT U/* gap */&"d\0061t""#] {
            let input = Tokenizer::new(&FelderaDialect, sql)
                .tokenize_with_location()
                .unwrap();
            assert_eq!(tokens(sql).unwrap(), input);
        }
    }
}
