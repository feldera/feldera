//! Scoped checks for syntax the upstream AST accepts beyond Feldera's grammar.

use sqlparser_064::dialect::Dialect;
use sqlparser_064::{
    ast::{
        DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
        ObjectType, Query, Select, Set, Spanned, Statement, TableFactor, Visit, Visitor,
    },
    parser::ParserError,
    tokenizer::{Location, Token, TokenWithSpan},
};
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

struct NoQueries;

pub(super) fn validate_non_query(expression: &Expr) -> Result<(), ParserError> {
    // Expression2 passes its enclosing context to the leading operand. The
    // right operand of a binary operator, CASE arms and ordinary function
    // arguments have their own ACCEPT_SUB_QUERY context. ROW and parentheses
    // propagate the enclosing context to each field.
    use sqlparser_064::ast::CastKind;
    match expression {
        Expr::Nested(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::BinaryOp { left: expr, .. }
        | Expr::Between { expr, .. }
        | Expr::In { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::ILike { expr, .. }
        | Expr::SimilarTo { expr, .. }
        | Expr::RLike { expr, .. }
        | Expr::Cast {
            kind: CastKind::DoubleColon,
            expr,
            ..
        } => return validate_non_query(expr),
        Expr::RowConstructor { fields, .. } => {
            return fields
                .iter()
                .try_for_each(|field| validate_non_query(&field.expr));
        }
        Expr::Tuple(fields) => return fields.iter().try_for_each(validate_non_query),
        Expr::Function(_)
        | Expr::Case { .. }
        | Expr::Array(_)
        | Expr::Map(_)
        | Expr::MapQuery { .. }
        | Expr::MultisetQuery { .. }
        | Expr::MultisetValue { .. }
        | Expr::Cast {
            kind: CastKind::Cast | CastKind::TryCast | CastKind::SafeCast,
            ..
        } => return Ok(()),
        _ => {}
    }
    // For other expression forms, reject embedded queries until their
    // operand grammar has been compared with Calcite.
    match expression.visit(&mut NoQueries) {
        ControlFlow::Break(error) => Err(error),
        ControlFlow::Continue(()) => Ok(()),
    }
}

impl Visitor for NoQueries {
    type Break = ParserError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break(super::diagnostic::at(
            "queries in non-query expressions are unsupported",
            query.span().start,
        ))
    }
}

/// Reject spellings that upstream erases before the AST guard can inspect them.
/// Upstream represents `==` as ordinary equality.
pub(super) fn validate_lossy_tokens(tokens: &[TokenWithSpan]) -> Result<(), ParserError> {
    if let Some(token) = tokens.iter().find(|token| token.token == Token::DoubleEq) {
        return Err(super::diagnostic::at(
            "double-equals syntax is outside the Feldera grammar",
            token.span.start,
        ));
    }
    Ok(())
}

pub(super) struct ScopedChecks {
    ambiguous_table_arguments: HashMap<Location, Location>,
    lambda_arguments: HashSet<(u64, u64)>,
    typed_literal_context: Vec<bool>,
    pub(super) statement_start: Location,
}

impl ScopedChecks {
    pub(super) fn new(tokens: &[TokenWithSpan]) -> Self {
        Self {
            ambiguous_table_arguments: super::grammar::ambiguous_table_arguments(tokens),
            lambda_arguments: HashSet::new(),
            typed_literal_context: Vec::new(),
            statement_start: Location::new(0, 0),
        }
    }

    fn allow_lambda_arguments(&mut self, args: &[FunctionArg]) {
        for arg in args {
            let (FunctionArg::Unnamed(arg)
            | FunctionArg::Named { arg, .. }
            | FunctionArg::ExprNamed { arg, .. }) = arg;
            if let FunctionArgExpr::Expr(Expr::Lambda(lambda)) = arg {
                if let Some(parameter) = lambda.params.first() {
                    let start = parameter.name.span.start;
                    self.lambda_arguments.insert((start.line, start.column));
                }
            }
        }
    }
}

impl Visitor for ScopedChecks {
    type Break = ParserError;

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        super::grammar::table_factor(table)?;
        match table {
            TableFactor::Table {
                args: Some(args), ..
            } => self.allow_lambda_arguments(&args.args),
            TableFactor::Function { args, .. } => self.allow_lambda_arguments(args),
            _ => (),
        }
        if let TableFactor::Table {
            name,
            with_hints,
            with_ordinality,
            ..
        } = table
        {
            let unnest = matches!(name.0.as_slice(), [ObjectNamePart::Identifier(identifier)]
                if identifier.quote_style.is_none() && identifier.value.eq_ignore_ascii_case("UNNEST"));
            if !with_hints.is_empty() || (*with_ordinality && !unnest) {
                return ControlFlow::Break(super::diagnostic::at(
                    "table hint or ordinality clause is outside the Feldera grammar",
                    name.span().start,
                ));
            }
        }
        if matches!(
            table,
            TableFactor::Function {
                with_ordinality: true,
                ..
            }
        ) {
            return ControlFlow::Break(super::diagnostic::at(
                "table-function ordinality is outside the Feldera grammar",
                table.span().start,
            ));
        }
        if let TableFactor::Table {
            name,
            args: Some(_),
            ..
        } = table
        {
            // This dialect currently retains UNNEST as a named table factor;
            // Calcite gives unquoted UNNEST its own unambiguous production.
            if matches!(name.0.as_slice(), [ObjectNamePart::Identifier(identifier)]
                if identifier.quote_style.is_none() && identifier.value.eq_ignore_ascii_case("UNNEST"))
            {
                return ControlFlow::Continue(());
            }
            if let Some(&argument_open) = self.ambiguous_table_arguments.get(&name.span().end) {
                return ControlFlow::Break(super::diagnostic::at(
                    "ambiguous first table-call argument requires parentheses or TABLE(...) syntax",
                    argument_open,
                ));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if matches!(statement, Statement::Insert(insert) if !insert.into) {
            return ControlFlow::Break(super::diagnostic::at(
                "INSERT requires INTO",
                self.statement_start,
            ));
        }
        super::ast_fields::statement(statement)?;
        if let Err(error) = super::attributes::validate(statement) {
            return ControlFlow::Break(error);
        }
        // Calcite SqlStmt plus Feldera's FMPP statement/create/drop extensions.
        // Default-deny new upstream statement families. This does not replace
        // validation of every option or expression within an allowed family.
        let supported = match statement {
            Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Delete(_)
            | Statement::Update(_)
            | Statement::Merge(_)
            | Statement::Call(_)
            | Statement::CreateTable(_)
            | Statement::CreateView(_)
            | Statement::CreateFunction(_)
            | Statement::CreateAggregate(_)
            | Statement::CreateIndex(_)
            | Statement::CreateType { .. }
            | Statement::Reset(_) => true,
            Statement::Set(Set::SingleAssignment {
                scope,
                hivevar,
                values,
                ..
            }) => scope.is_none() && !hivevar && values.len() == 1,
            Statement::Drop {
                object_type,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
                ..
            } => {
                matches!(object_type, ObjectType::Table | ObjectType::View)
                    && names.len() == 1
                    && !cascade
                    && !restrict
                    && !purge
                    && !temporary
                    && table.is_none()
            }
            _ => false,
        };
        if supported {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(super::diagnostic::at(
                "statement family is outside the Feldera parser grammar",
                self.statement_start,
            ))
        }
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        self.typed_literal_context
            .push(matches!(expression, Expr::TypedString(_)));
        super::grammar::expression(expression)?;
        // Calcite admits lambdas as direct function arguments, not as arbitrary
        // value expressions. Token locations identify these original AST nodes.
        if let Expr::Lambda(lambda) = expression {
            let start = lambda
                .params
                .first()
                .map_or(self.statement_start, |p| p.name.span.start);
            if !self.lambda_arguments.contains(&(start.line, start.column)) {
                return ControlFlow::Break(super::diagnostic::at(
                    "lambda expressions require a direct function argument context",
                    start,
                ));
            }
        }
        if let Expr::Function(function) = expression {
            for arguments in [&function.args, &function.parameters] {
                if let FunctionArguments::List(arguments) = arguments {
                    self.allow_lambda_arguments(&arguments.args);
                }
            }
        }
        if let Expr::TypedString(literal) = expression {
            if matches!(literal.value.value, sqlparser_064::ast::Value::Null) {
                return ControlFlow::Break(super::diagnostic::at(
                    "typed literals require a string value",
                    literal.value.span.start,
                ));
            }
        }
        let Expr::Function(function) = expression else {
            return ControlFlow::Continue(());
        };
        // Upstream attaches suffix null treatment after FILTER. Calcite's
        // grammar requires it before FILTER; inline argument treatment is
        // represented separately and remains valid for its supported functions.
        if function.filter.is_some() && function.null_treatment.is_some() {
            return ControlFlow::Break(super::diagnostic::at(
                "suffix null treatment after FILTER is unsupported",
                expression.span().start,
            ));
        }
        // The upstream expression parser can add a schema prefix after parsing
        // the call. Check the fully qualified name once it is available.
        if !super::FelderaDialect.supports_inline_null_treatment_for(&function.name.0)
            && matches!(&function.args, FunctionArguments::List(list) if list.clauses.iter().any(|clause|
                matches!(clause, sqlparser_064::ast::FunctionArgumentClause::IgnoreOrRespectNulls(_))))
        {
            return ControlFlow::Break(super::diagnostic::at(
                "qualified functions do not accept inline null treatment",
                expression.span().start,
            ));
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expression: &Expr) -> ControlFlow<Self::Break> {
        self.typed_literal_context
            .pop()
            .expect("expression context was pushed");
        ControlFlow::Continue(())
    }

    fn pre_visit_data_type(&mut self, data_type: &DataType) -> ControlFlow<Self::Break> {
        // DATETIME is a literal introducer but not a type declaration in
        // Calcite. The literal production supplies its own restricted tags.
        if self.typed_literal_context.last() == Some(&true) {
            return ControlFlow::Continue(());
        }
        super::grammar::data_type(data_type, self.statement_start)
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        super::ast_fields::query(query)
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        super::ast_fields::select(select)?;
        super::grammar::select(select)
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn rejections_use_original_multistatement_and_unicode_locations() {
        for (sql, line, column) in [
            ("SELECT 1;\n -- prior statement\n COMMIT", 3, 2),
            ("SELECT 1;\nSELECT EXTRACT(YEAR FROMx)", 2, 21),
            ("SELECT 1;\nSELECT transform(a, b INT -> b+1)", 2, 21),
            ("SELECT 1;\nSET x = 1 + 2", 2, 9),
            ("SELECT '🦀';\nSELECT U&\"broken\\\"", 2, 8),
            ("DECLARE RECURSIVE VIEW v(x INT) trailing", 1, 33),
            ("SELECT 1;\nLATENESS v.x  ", 2, 15),
            ("SELECT 1;\nSELECT (\n  ", 3, 3),
            ("CREATE TABLE t (n INT CONSTRAINT c LATENESS 1)", 1, 17),
            ("CREATE VIEW v(n LATENESS 1) AS SELECT 1", 1, 15),
            ("ALTER TABLE t ADD COLUMN n INT LATENESS 1", 1, 26),
            ("SELECT * FROM a RIGHT ASOF JOIN b ON TRUE", 1, 23),
        ] {
            let Err(error) = parse(sql) else {
                panic!("expected rejection: {sql}")
            };
            assert!(
                error
                    .to_string()
                    .ends_with(&format!("Line: {line}, Column: {column}")),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn unsupported_statement_families_cannot_hide_in_a_valid_program() {
        for statement in [
            "COMMIT",
            "ROLLBACK",
            "TRUNCATE TABLE t",
            "SHOW TABLES",
            "CREATE SCHEMA s",
            "CREATE ROLE r",
            "DROP INDEX i",
            "ALTER TABLE t ADD COLUMN n INT",
        ] {
            for prefix in ["", "SELECT 1; "] {
                let sql = format!("{prefix}{statement}");
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t WHERE n=1",
            "UPDATE t SET n=1",
            "CREATE TABLE t(n INT)",
            "CREATE VIEW v AS SELECT 1",
            "DROP TABLE t",
            "DROP VIEW v",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn interval_qualifiers_never_become_projection_aliases() {
        for unit in [
            "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
        ] {
            for spelling in [unit.to_owned(), format!("{unit}S")] {
                assert!(parse(&format!("SELECT (a-b) {spelling} FROM t")).is_ok());
                assert!(parse(&format!("SELECT (a-b) AS {spelling} FROM t")).is_ok());
                assert!(parse(&format!("SELECT (a-b) \"{spelling}\" FROM t")).is_ok());
            }
        }
        for query in [
            "SELECT (a - b) YEAR FROM t",
            "SELECT ((a - b)) MONTH FROM t",
            "SELECT * FROM (SELECT (a-b) HOUR FROM t) q",
            "SELECT 1 UNION SELECT (a-b) MINUTE FROM t",
            "CREATE VIEW v AS SELECT (a-b) /* comment */ YEAR FROM t",
        ] {
            assert!(parse(query).is_ok(), "{query}");
        }
        for query in [
            "SELECT (a-b) AS year FROM t",
            "SELECT (a-b) \"YEAR\" FROM t",
            "SELECT f(a) YEAR FROM t",
            "SELECT (a-b) ordinary_alias FROM t",
            "SELECT year FROM t",
        ] {
            assert!(parse(query).is_ok(), "{query}");
        }
    }
}
