//! Register SQL type aliases before resolving the tables that reference them.
//! An alias definition can use earlier aliases in the same batch.

use super::{
    DeclarationTokens, ParsedStatement, StatementText,
    types::{TypeSpelling, declare_type},
};
use crate::program::{diagnostic_source, source::coordinates};
use crate::{Failure, unsupported};
use feldera_sql_parser::TypeDefinition;
use feldera_sql_rel_model::{
    TypeId, ValidationLimits, catalog::Name, diagnostics::DiagnosticBoundary, program::Program,
    source::Origin, statements::Statement,
};
use sqlparser_064::{
    ast::{Ident, ObjectName, ObjectNamePart},
    tokenizer::Span,
};
use std::collections::HashMap;

pub(super) type Aliases = HashMap<String, TypeId>;

pub(super) fn type_name(name: &ObjectName) -> Result<(Span, Name, TypeSpelling), Failure> {
    let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
        return Err(unsupported("expected a local type name"));
    };
    let (name, spelling) = identifier(ident);
    Ok((ident.span, name, spelling))
}

pub(super) fn identifier(ident: &Ident) -> (Name, TypeSpelling) {
    let name = Name {
        name: if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        },
        quoted: ident.quote_style.is_some(),
    };
    let spelling = TypeSpelling {
        quoted: format!("`{}`", name.name.replace('`', "``")),
        diagnostic: if name.quoted {
            format!("\"{}\"", name.name.replace('"', "\"\""))
        } else {
            name.name.clone()
        },
    };
    (name, spelling)
}

pub(super) fn register(
    program: &mut Program,
    parsed: &[ParsedStatement],
    tokens: &[DeclarationTokens],
) -> Result<Aliases, Failure> {
    let mut aliases = Aliases::new();
    let mut statements = Vec::new();
    for (parsed, tokens) in parsed.iter().zip(tokens) {
        let ParsedStatement::CreateType(decl) = parsed else {
            continue;
        };
        if decl.replace {
            return Err(unsupported("CREATE OR REPLACE TYPE"));
        }
        let (span, name, spelling) = type_name(&decl.name)?;
        let types = &mut program.table.type_table;
        let depth = ValidationLimits::default().depth;
        let (ty, target, attributes) = match &decl.definition {
            TypeDefinition::Alias(target) => {
                let (ty, spelling) = declare_type(types, &aliases, target, false, depth)?;
                if matches!(
                    types.types[ty.0 as usize].kind,
                    feldera_sql_rel_model::types::TypeKind::Row { .. }
                ) {
                    return Err(unsupported(
                        "a ROW type declaration requires named attributes",
                    ));
                }
                (ty, spelling, None)
            }
            TypeDefinition::Struct(fields) => {
                let mut record = super::types::RecordFields::default();
                let mut attributes = Vec::with_capacity(fields.len());
                for field in fields {
                    if field.default.is_some() {
                        return Err(unsupported("structured attribute DEFAULT"));
                    }
                    // CREATE TYPE attributes default to nullable. An inline ROW defaults to NOT NULL.
                    let nullable = field.nullable.unwrap_or(true);
                    let (ty, spelling) =
                        declare_type(types, &aliases, &field.data_type, nullable, depth)?;
                    let named_type = super::types::declared_type_name(&field.data_type)?;
                    let name = record.push(
                        &field.name,
                        ty,
                        spelling,
                        if nullable { "" } else { " NOT NULL" },
                    )?;
                    attributes.push(feldera_sql_rel_model::statements::Attribute {
                        name,
                        named_type,
                        nullable: Some(nullable),
                    });
                }
                let (kind, spelling) = record.finish("");
                let ty = super::super::graph::insert_type(
                    types,
                    feldera_sql_rel_model::types::SqlType {
                        nullable: false,
                        origin: Origin::Absent {},
                        kind,
                    },
                );
                (ty, spelling, Some(attributes))
            }
        };
        if aliases.insert(name.name.clone(), ty).is_some() {
            return Err(unsupported(format!("duplicate type {}", name.name)));
        }
        let text = StatementText {
            position: tokens.range()?,
            pretty: format!("CREATE TYPE {} AS {}", spelling.quoted, target.quoted),
            diagnostic: format!(
                "CREATE TYPE {} AS {}",
                spelling.diagnostic, target.diagnostic
            ),
        };
        statements.push(Statement::CreateType {
            source: text.source("CREATE_TYPE"),
            name,
            name_diagnostic: diagnostic_source(&coordinates(span), &spelling.diagnostic),
            ty,
            attributes,
        });
    }
    let boundary = DiagnosticBoundary {
        context: Origin::Absent {},
        messages: vec![],
    };
    program
        .diagnostics
        .before
        .splice(0..0, std::iter::repeat_n(boundary, statements.len()));
    program.statements.splice(0..0, statements);
    Ok(aliases)
}
