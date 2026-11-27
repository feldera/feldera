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
    ast::{ObjectName, ObjectNamePart},
    tokenizer::Span,
};
use std::collections::HashMap;

pub(super) type Aliases = HashMap<String, TypeId>;

pub(super) fn type_name(name: &ObjectName) -> Result<(Span, Name, TypeSpelling), Failure> {
    let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
        return Err(unsupported("expected a local type name"));
    };
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
    Ok((ident.span, name, spelling))
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
        let TypeDefinition::Alias(target) = &decl.definition else {
            return Err(unsupported(
                "structured type declarations require attribute binding",
            ));
        };
        if decl.replace {
            return Err(unsupported("CREATE OR REPLACE TYPE"));
        }
        let (span, name, spelling) = type_name(&decl.name)?;
        let (ty, target) = declare_type(
            &mut program.table.type_table,
            &aliases,
            target,
            false,
            ValidationLimits::default().depth,
        )?;
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
            attributes: None,
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
