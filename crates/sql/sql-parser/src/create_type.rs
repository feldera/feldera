//! Scalar aliases and structured type declarations in the Feldera DDL grammar.

use crate::statements::identifier;
use sqlparser_064::{
    ast::{DataType, Expr, Ident, ObjectName, ObjectNamePart, Visit, Visitor},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Span, Token},
};
use std::{fmt, ops::ControlFlow};

#[derive(Debug)]
pub struct CreateType {
    pub name: ObjectName,
    pub replace: bool,
    pub definition: TypeDefinition,
    pub span: Span,
}

#[derive(Debug)]
pub enum TypeDefinition {
    Alias(DataType),
    Struct(Vec<Attribute>),
}

#[derive(Debug)]
pub struct Attribute {
    pub name: Ident,
    pub data_type: DataType,
    /// None means the SQL omitted the modifier, which defaults to nullable.
    pub nullable: Option<bool>,
    pub default: Option<Expr>,
}

impl CreateType {
    pub(crate) fn parse(parser: &mut Parser) -> Result<Self, ParserError> {
        let start = parser.peek_token().span.start;
        parser.expect_keyword(Keyword::CREATE)?;
        let replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        parser.expect_keyword(Keyword::TYPE)?;
        let mut parts = vec![ObjectNamePart::Identifier(identifier(parser)?)];
        while parser.consume_token(&Token::Period) {
            parts.push(ObjectNamePart::Identifier(identifier(parser)?));
        }
        parser.expect_keyword(Keyword::AS)?;
        let definition = if parser.consume_token(&Token::LParen) {
            let attributes = parser.parse_comma_separated(attribute)?;
            parser.expect_token(&Token::RParen)?;
            TypeDefinition::Struct(attributes)
        } else {
            TypeDefinition::Alias(parser.parse_data_type()?)
        };
        Ok(Self {
            name: ObjectName(parts),
            replace,
            definition,
            span: Span::new(start, parser.token_at(parser.index() - 1).span.end),
        })
    }

    pub(crate) fn visit_fields<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        match &self.definition {
            TypeDefinition::Alias(ty) => ty.visit(visitor),
            TypeDefinition::Struct(fields) => fields.iter().try_for_each(|field| {
                field.data_type.visit(visitor)?;
                field.default.visit(visitor)
            }),
        }
    }
}

fn attribute(parser: &mut Parser) -> Result<Attribute, ParserError> {
    let name = identifier(parser)?;
    let data_type = parser.parse_data_type()?;
    let nullable = if parser.parse_keyword(Keyword::NOT) {
        parser.expect_keyword(Keyword::NULL)?;
        Some(false)
    } else {
        parser.parse_keyword(Keyword::NULL).then_some(true)
    };
    let default = if parser.parse_keyword(Keyword::DEFAULT) {
        Some(parser.parse_expr()?)
    } else {
        None
    };
    Ok(Attribute {
        name,
        data_type,
        nullable,
        default,
    })
}

impl fmt::Display for CreateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let replace = if self.replace { " OR REPLACE" } else { "" };
        write!(f, "CREATE{replace} TYPE {} AS ", self.name)?;
        match &self.definition {
            TypeDefinition::Alias(ty) => ty.fmt(f),
            TypeDefinition::Struct(fields) => {
                f.write_str("(")?;
                for (index, field) in fields.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{} {}", field.name, field.data_type)?;
                    match field.nullable {
                        Some(false) => f.write_str(" NOT NULL")?,
                        Some(true) => f.write_str(" NULL")?,
                        None => {}
                    }
                    if let Some(expr) = &field.default {
                        write!(f, " DEFAULT {expr}")?;
                    }
                }
                f.write_str(")")
            }
        }
    }
}
