//! Lexical adjacency for charset introducers that include hyphens.
//!
//! Calcite treats `_UTF-8'text'` as one prefixed literal. Without this token
//! join, sqlparser reads subtraction followed by a string projection alias.

use sqlparser_064::tokenizer::{Token, TokenWithSpan};

pub(super) fn join_charset_prefixes(tokens: Vec<TokenWithSpan>) -> Vec<TokenWithSpan> {
    let mut result = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while index < tokens.len() {
        let Token::Word(word) = &tokens[index].token else {
            result.push(tokens[index].clone());
            index += 1;
            continue;
        };
        if word.quote_style.is_some() || !word.value.starts_with('_') {
            result.push(tokens[index].clone());
            index += 1;
            continue;
        }
        let mut end = index;
        let mut name = word.value.clone();
        while let (Some(hyphen), Some(part)) = (tokens.get(end + 1), tokens.get(end + 2)) {
            if hyphen.token != Token::Minus
                || tokens[end].span.end != hyphen.span.start
                || hyphen.span.end != part.span.start
            {
                break;
            }
            let text = match &part.token {
                Token::Word(word) if word.quote_style.is_none() => &word.value,
                Token::Number(number, false) if number.chars().all(|c| c.is_ascii_digit()) => {
                    number
                }
                _ => break,
            };
            name.push('-');
            name.push_str(text);
            end += 2;
        }
        if end != index
            && tokens.get(end + 1).is_some_and(|next| {
                matches!(next.token, Token::SingleQuotedString(_))
                    && tokens[end].span.end == next.span.start
            })
        {
            let mut token = tokens[index].clone();
            token.token = Token::make_word(&name, None);
            token.span.end = tokens[end].span.end;
            result.push(token);
            index = end + 1;
        } else {
            result.push(tokens[index].clone());
            index += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::statements;
    use sqlparser_064::ast::{Expr, SelectItem, SetExpr, Statement, Value};

    fn projection(sql: &str) -> SelectItem {
        let nodes = crate::parse(sql).unwrap();
        let statements::Statement::Upstream(statement) = &nodes[0] else {
            panic!("upstream statement")
        };
        let Statement::Query(query) = statement.as_ref() else {
            panic!("query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        select.projection[0].clone()
    }

    #[test]
    fn charset_adjacency_preserves_literal_values_and_original_identifier_spans() {
        for name in ["_utf8", "_utf-8", "_utf-16be", "_iso-8859-1"] {
            let SelectItem::UnnamedExpr(Expr::Prefixed { prefix, value }) =
                projection(&format!("SELECT {name}'a''b'"))
            else {
                panic!("charset literal {name}")
            };
            assert_eq!(prefix.value, name);
            assert_eq!(prefix.span.start.column, 8);
            assert_eq!(prefix.span.end.column, 8 + name.len() as u64);
            assert!(matches!(value.as_ref(), Expr::Value(v)
                if v.value == Value::SingleQuotedString("a'b".into())));
            let emitted = Expr::Prefixed { prefix, value }.to_string();
            assert_eq!(emitted, format!("{name}'a''b'"));
        }
        // A gap changes the grammar: this is subtraction with a string alias.
        assert!(matches!(
            projection("SELECT _utf-8 'abc'"),
            SelectItem::ExprWithAlias {
                expr: Expr::BinaryOp { .. },
                ..
            }
        ));
        assert!(matches!(
            projection("SELECT _utf8 'abc'"),
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                ..
            }
        ));
    }

    #[test]
    fn escape_literals_decode_instead_of_becoming_identifiers_and_aliases() {
        let SelectItem::UnnamedExpr(Expr::Value(value)) = projection("SELECT E'a\\nb'") else {
            panic!("escape literal")
        };
        assert_eq!(value.value, Value::EscapedStringLiteral("a\nb".into()));
        for text in [r"a\vb", r"\q"] {
            let SelectItem::UnnamedExpr(Expr::Value(value)) =
                projection(&format!("SELECT E'{text}'"))
            else {
                panic!("unknown escape")
            };
            assert_eq!(value.value, Value::EscapedStringLiteral(text.into()));
        }
        assert!(matches!(
            projection("SELECT E 'a\\nb'"),
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                ..
            }
        ));
    }
}
