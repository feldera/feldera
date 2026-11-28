//! Feldera's table elements and column attributes from the extended CREATE TABLE grammar.

use crate::statements::{compound_name, identifier};
use sqlparser_064::{
    ast::{
        helpers::stmt_create_table::CreateTableBuilder, ColumnDef, ColumnOption, ColumnOptionDef,
        CreateTable, CreateTableOptions, DataType, ForeignKeyConstraint, Ident,
        PrimaryKeyConstraint, Spanned, Statement, TableConstraint,
    },
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};
use std::fmt::{self, Display};

/// sqlparser stores constraints separately from columns. Their spans retain
/// the order of the table elements in Calcite's parse tree.
pub(crate) fn display(table: &CreateTable, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if table.constraints.is_empty() || !super::table(table) {
        return table.fmt(f);
    }
    f.write_str("CREATE ")?;
    if table.or_replace {
        f.write_str("OR REPLACE ")?;
    }
    f.write_str("TABLE ")?;
    if table.if_not_exists {
        f.write_str("IF NOT EXISTS ")?;
    }
    write!(f, "{} (", table.name)?;
    let mut constraints = table.constraints.iter().peekable();
    let mut separator = "";
    for column in &table.columns {
        while let Some(key) = constraints.next_if(|key| key.span().start < column.name.span.start) {
            write!(f, "{separator}{key}")?;
            separator = ", ";
        }
        write!(f, "{separator}{column}")?;
        separator = ", ";
    }
    for key in constraints {
        write!(f, "{separator}{key}")?;
        separator = ", ";
    }
    f.write_str(")")?;
    if !matches!(table.table_options, CreateTableOptions::None) {
        write!(f, " {}", table.table_options)?;
    }
    Ok(())
}

pub(super) fn declaration(parser: &mut Parser, replace: bool) -> Result<Statement, ParserError> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = compound_name(parser)?;
    parser.expect_token(&Token::LParen)?;
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    loop {
        if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            constraints.push(TableConstraint::PrimaryKey(primary_key(names(parser)?)));
        } else if parser.parse_keywords(&[Keyword::FOREIGN, Keyword::KEY]) {
            let columns = names(parser)?;
            parser.expect_keyword(Keyword::REFERENCES)?;
            let foreign_table = identifier(parser)?.into();
            constraints.push(TableConstraint::ForeignKey(ForeignKeyConstraint {
                columns,
                foreign_table,
                referred_columns: names(parser)?,
                name: None,
                index_name: None,
                on_delete: None,
                on_update: None,
                match_kind: None,
                characteristics: None,
            }));
        } else {
            columns.push(column(parser)?);
        }
        if !parser.consume_token(&Token::Comma) || parser.peek_token_ref().token == Token::RParen {
            break;
        }
    }
    parser.expect_token(&Token::RParen)?;
    let options = super::properties::parse(parser)?;
    Ok(Statement::CreateTable(
        CreateTableBuilder::new(name)
            .or_replace(replace)
            .if_not_exists(if_not_exists)
            .columns(columns)
            .constraints(constraints)
            .table_options(options)
            .build(),
    ))
}

pub(super) fn names(parser: &mut Parser) -> Result<Vec<Ident>, ParserError> {
    parser.expect_token(&Token::LParen)?;
    let names = parser.parse_comma_separated(identifier)?;
    parser.expect_token(&Token::RParen)?;
    Ok(names)
}

fn primary_key(columns: Vec<Ident>) -> PrimaryKeyConstraint {
    PrimaryKeyConstraint {
        name: None,
        index_name: None,
        index_type: None,
        columns: columns.into_iter().map(Into::into).collect(),
        include: Vec::new(),
        index_options: Vec::new(),
        characteristics: None,
    }
}

fn column(parser: &mut Parser) -> Result<ColumnDef, ParserError> {
    let name = identifier(parser)?;
    let mut options = Vec::new();
    if matches!(parser.peek_token_ref().token, Token::Comma | Token::RParen) {
        return Ok(ColumnDef {
            name,
            data_type: DataType::Unspecified,
            options,
        });
    }
    let data_type = parser.parse_data_type()?;
    let nullability = if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
        Some(ColumnOption::NotNull)
    } else {
        parser
            .parse_keyword(Keyword::NULL)
            .then_some(ColumnOption::Null)
    };
    options.extend(nullability.map(|option| ColumnOptionDef { name: None, option }));
    loop {
        let start = parser.peek_token_ref().span;
        let option = if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            ColumnOption::PrimaryKey(primary_key(Vec::new()))
        } else if parser.parse_keywords(&[Keyword::FOREIGN, Keyword::KEY, Keyword::REFERENCES]) {
            super::inline_foreign_key(parser, start)?
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            ColumnOption::Default(parser.parse_expr()?)
        } else if let Some(option) = crate::lateness::parse_option(parser)? {
            option
        } else {
            break;
        };
        options.push(ColumnOptionDef { name: None, option });
    }
    Ok(ColumnDef {
        name,
        data_type,
        options,
    })
}
