//! Local grammar constraints on otherwise valid upstream AST nodes.
//!
//! These checks run recursively, including inside subqueries and nested types.
//! They are not an exhaustive replacement for the pinned Calcite grammar.

use sqlparser_064::{
    ast::{
        AccessExpr, BinaryOperator, DataType, Expr, FunctionArgumentClause, FunctionArguments,
        GroupByExpr, NamedWindowExpr, ObjectNamePart, PivotValueSource, Select, Spanned, Subscript,
        TableFactor, TableSample, TableSampleKind, TableSampleMethod, TableSampleModifier,
        TableSampleQuantity, TableSampleSeed, TableSampleSeedModifier, UnaryOperator, Value,
        WindowFrameUnits, WindowSpec, WindowType,
    },
    parser::ParserError,
    tokenizer::{Location, Token, TokenWithSpan},
};
use std::{collections::HashMap, ops::ControlFlow};

pub(super) fn ambiguous_table_arguments(tokens: &[TokenWithSpan]) -> HashMap<Location, Location> {
    let significant: Vec<_> = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .collect();
    significant
        .windows(4)
        .filter_map(|pair| {
            let Token::Word(word) = &pair[2].token else {
                return None;
            };
            // Named calls overlap Calcite's EXTEND lookahead. These dedicated
            // expression productions do not have that ambiguity.
            let dedicated = word.quote_style.is_none()
                && matches!(
                    word.value.to_ascii_uppercase().as_str(),
                    "CAST"
                        | "TRY_CAST"
                        | "SAFE_CAST"
                        | "ROW"
                        | "DATEADD"
                        | "DATEDIFF"
                        | "DATEPART"
                        | "DATE_PART"
                        | "EXTRACT"
                        | "POSITION"
                        | "CONVERT"
                        | "TRANSLATE"
                        | "OVERLAY"
                        | "FLOOR"
                        | "CEIL"
                        | "CEILING"
                        | "SUBSTRING"
                        | "TRIM"
                        | "TUMBLE"
                        | "HOP"
                        | "SESSION"
                        | "ARRAY"
                        | "MAP"
                        | "MULTISET"
                        | "JSON_EXISTS"
                        | "JSON_VALUE"
                        | "JSON_QUERY"
                        | "JSON_OBJECT"
                        | "JSON_OBJECTAGG"
                        | "JSON_ARRAY"
                        | "JSON_ARRAYAGG"
                );
            (matches!(pair[0].token, Token::Word(_))
                && pair[1].token == Token::LParen
                && pair[3].token == Token::LParen
                && !dedicated)
                .then_some((pair[0].span.end, pair[3].span.start))
        })
        .collect()
}

pub(super) fn select(select: &Select) -> ControlFlow<ParserError> {
    if select.named_window.iter().any(
        |window| matches!(&window.1, NamedWindowExpr::WindowSpec(spec) if !window_allowed(spec)),
    ) {
        return ControlFlow::Break(super::diagnostic::at(
            "GROUPS window frames are outside the Feldera grammar",
            select.span().start,
        ));
    }
    let has_group = !matches!(&select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty());
    // SqlSelect nests all five clauses inside its optional FROM production.
    let needs_from = select.selection.is_some()
        || has_group
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some();
    let reason = if select.projection.is_empty() {
        "SELECT requires a projection"
    } else if select.from.is_empty() && needs_from {
        "WHERE, GROUP BY, HAVING, WINDOW and QUALIFY require FROM"
    } else {
        return ControlFlow::Continue(());
    };
    ControlFlow::Break(super::diagnostic::at(reason, select.span().start))
}

pub(super) fn expression(expression: &Expr) -> ControlFlow<ParserError> {
    if !super::expression_policy::allows(expression) {
        return ControlFlow::Break(super::diagnostic::at(
            "expression field is outside the supported Feldera grammar",
            expression.span().start,
        ));
    }
    let reason = match expression {
        Expr::Collate { .. } => "expression COLLATE is outside the Feldera grammar",
        Expr::UnaryOp {
            op: UnaryOperator::BitwiseNot,
            ..
        } => "bitwise NOT syntax is outside the Feldera grammar",
        Expr::BinaryOp {
            op:
                BinaryOperator::Assignment
                | BinaryOperator::Arrow
                | BinaryOperator::LongArrow
                | BinaryOperator::HashArrow
                | BinaryOperator::HashLongArrow
                | BinaryOperator::AtAt
                | BinaryOperator::AtArrow
                | BinaryOperator::ArrowAt
                | BinaryOperator::AtQuestion
                | BinaryOperator::PGLikeMatch
                | BinaryOperator::PGNotLikeMatch,
            ..
        } => "PostgreSQL operator syntax is outside the Feldera grammar",
        Expr::Like { any: true, .. } | Expr::ILike { any: true, .. } => {
            "LIKE ANY is outside the Feldera grammar"
        }
        Expr::CompoundFieldAccess { access_chain, .. }
            if access_chain
                .iter()
                .any(|access| matches!(access, AccessExpr::Subscript(Subscript::Slice { .. }))) =>
        {
            "array slices are outside the Feldera grammar"
        }
        Expr::Function(function) if !function_arguments_allowed(function) => {
            "function argument clause is outside the Feldera grammar"
        }
        Expr::Function(function)
            if matches!(&function.over,
            Some(WindowType::WindowSpec(spec)) if !window_allowed(spec)) =>
        {
            "GROUPS window frames are outside the Feldera grammar"
        }
        Expr::CompoundFieldAccess { access_chain, .. }
            if access_chain.iter().any(|access| {
                matches!(access,
                AccessExpr::Dot(Expr::Value(value)) if matches!(value.value, Value::Number(..)))
            }) =>
        {
            "numeric field names require quotes"
        }
        Expr::JsonAccess { .. } => "colon access is outside the Feldera grammar",
        Expr::BinaryOp {
            op: BinaryOperator::BitwiseOr,
            ..
        } => "bitwise OR syntax is outside the Feldera grammar",
        Expr::Interval(interval) if interval.leading_field.is_none() => "INTERVAL requires a unit",
        Expr::Interval(interval)
            if matches!(interval.value.as_ref(),
            Expr::Value(value) if matches!(&value.value,
                Value::SingleQuotedString(text) if text.is_empty())) =>
        {
            "INTERVAL requires a nonempty literal"
        }
        Expr::Lambda(lambda) if lambda.params.iter().any(|param| param.data_type.is_some()) => {
            "lambda parameters cannot declare types"
        }
        Expr::Function(function)
            if matches!(function.name.0.as_slice(),
            [ObjectNamePart::Identifier(name)]
                if name.quote_style.is_none() && matches!(name.value.to_ascii_uppercase().as_str(), "IN" | "FROM" | "ROLLUP" | "EXTRACT" | "CONVERT")) =>
        {
            "reserved keyword is not a function name"
        }
        _ => return ControlFlow::Continue(()),
    };
    // Upstream's Spanned implementation has no Lambda arm. Use its original
    // parameter token, rather than an empty expression span or invented offset.
    let start = match expression {
        Expr::Lambda(lambda) => lambda
            .params
            .first()
            .map_or(expression.span().start, |parameter| {
                parameter.name.span.start
            }),
        _ => expression.span().start,
    };
    ControlFlow::Break(super::diagnostic::at(reason, start))
}

pub(super) fn table_factor(table: &TableFactor) -> ControlFlow<ParserError> {
    if let TableFactor::GroupedUnpivot {
        measures,
        axes,
        columns,
        ..
    } = table
    {
        for name in measures
            .iter()
            .chain(axes)
            .chain(columns.iter().flat_map(|group| &group.columns))
        {
            super::reserved::identifier(name)?;
        }
    }
    if let TableFactor::Pivot {
        value_source,
        default_on_null,
        ..
    } = table
    {
        if !matches!(value_source, PivotValueSource::List(_)) || default_on_null.is_some() {
            return ControlFlow::Break(super::diagnostic::at(
                "dynamic PIVOT or DEFAULT ON NULL is outside the Feldera grammar",
                table.span().start,
            ));
        }
    }
    let sample = match table {
        TableFactor::Table { sample, .. } | TableFactor::Derived { sample, .. } => sample,
        _ => return ControlFlow::Continue(()),
    };
    let Some(TableSampleKind::BeforeTableAlias(sample) | TableSampleKind::AfterTableAlias(sample)) =
        sample
    else {
        return ControlFlow::Continue(());
    };
    let TableSample {
        modifier,
        name,
        quantity,
        seed,
        bucket,
        offset,
    } = sample.as_ref();
    let quantity_allowed = quantity.as_ref().is_some_and(
        |TableSampleQuantity {
             parenthesized,
             value,
             unit,
         }| {
            *parenthesized
                && unit.is_none()
                && matches!(value, Expr::Value(v) if matches!(v.value, Value::Number(..)))
        },
    );
    let seed_allowed = seed.as_ref().is_none_or(|TableSampleSeed { modifier, value }|
        *modifier == TableSampleSeedModifier::Repeatable
            && matches!(&value.value, Value::Number(number, false)
                if !number.is_empty() && number.trim_start_matches('-').chars().all(|c| c.is_ascii_digit())));
    if *modifier == TableSampleModifier::TableSample
        && matches!(
            name,
            Some(TableSampleMethod::Bernoulli | TableSampleMethod::System)
        )
        && quantity_allowed
        && seed_allowed
        && bucket.is_none()
        && offset.is_none()
    {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(super::diagnostic::at(
            "TABLESAMPLE option is outside the Feldera grammar",
            table.span().start,
        ))
    }
}

fn window_allowed(spec: &WindowSpec) -> bool {
    spec.window_frame
        .as_ref()
        .is_none_or(|frame| frame.units != WindowFrameUnits::Groups)
}

fn function_arguments_allowed(function: &sqlparser_064::ast::Function) -> bool {
    let json_constructor = matches!(function.name.0.as_slice(),
        [ObjectNamePart::Identifier(name)] if name.quote_style.is_none()
            && matches!(name.value.to_ascii_uppercase().as_str(),
                "JSON_ARRAY" | "JSON_OBJECT" | "JSON_ARRAYAGG" | "JSON_OBJECTAGG"));
    [&function.args, &function.parameters]
        .into_iter()
        .all(|args| {
            let FunctionArguments::List(args) = args else {
                return true;
            };
            args.clauses.iter().all(|clause| match clause {
                FunctionArgumentClause::Where(_)
                | FunctionArgumentClause::Limit(_)
                | FunctionArgumentClause::OnOverflow(_) => false,
                FunctionArgumentClause::JsonNullClause(_) => json_constructor,
                _ => true,
            })
        })
}

pub(super) fn data_type(data_type: &DataType, fallback: Location) -> ControlFlow<ParserError> {
    if !super::type_policy::allows(data_type) {
        return ControlFlow::Break(super::diagnostic::at(
            "type parameter or shape is outside the Feldera grammar",
            fallback,
        ));
    }
    let DataType::Custom(name, modifiers) = data_type else {
        return ControlFlow::Continue(());
    };
    for part in &name.0 {
        if let ObjectNamePart::Identifier(identifier) = part {
            // These built-in Calcite types are represented as Custom upstream.
            if name.0.len() == 1
                && identifier.quote_style.is_none()
                && matches!(
                    identifier.value.to_ascii_uppercase().as_str(),
                    "ANY" | "VARIANT"
                )
            {
                continue;
            }
            super::reserved::identifier(identifier)?;
        }
    }
    if !modifiers.is_empty() {
        let start = name.span().start;
        return ControlFlow::Break(super::diagnostic::at(
            "user-defined types cannot have parameters",
            if start.line == 0 { fallback } else { start },
        ));
    }
    ControlFlow::Continue(())
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn foreign_operators_and_dynamic_pivots_are_rejected_without_losing_controls() {
        for expression in [
            "a -> b",
            "a ->> b",
            "a #> b",
            "a #>> b",
            "a @> b",
            "a <@ b",
            "a @@ b",
            "a @? b",
            "a ~~ b",
            "a !~~ b",
            "~a",
            "a == b",
            "f(x := 1)",
        ] {
            for sql in [
                format!("SELECT {expression} FROM t"),
                format!("SELECT * FROM (SELECT {expression} FROM t) q"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for expression in [
            "a = b",
            "a ~ b",
            "a ~* b",
            "a !~ b",
            "a !~* b",
            "f(x => 1)",
            "f(x -> x + 1)",
            "NOT a",
            "-a",
        ] {
            let sql = format!("SELECT {expression} FROM t");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
        for clause in [
            "IN (ANY)",
            "IN (SELECT y FROM s)",
            "IN (1,2) DEFAULT ON NULL (0)",
        ] {
            assert!(!parse(&format!("SELECT * FROM t PIVOT(sum(x) FOR y {clause})")).is_ok());
        }
        assert!(parse("SELECT * FROM t PIVOT(sum(x) FOR y IN (1,2))").is_ok());
    }

    #[test]
    fn sample_fields_and_expression_collations_follow_calcite_grammar() {
        for clause in [
            "TABLESAMPLE (10 ROWS)",
            "TABLESAMPLE (10 PERCENT)",
            "TABLESAMPLE (BUCKET 1 OUT OF 10 ON x)",
            "TABLESAMPLE SYSTEM (1 + 2)",
            "TABLESAMPLE SYSTEM (10) SEED (1)",
        ] {
            assert!(!parse(&format!("SELECT * FROM t {clause}")).is_ok());
        }
        assert!(!parse("SELECT x COLLATE en_US FROM t").is_ok());
        for clause in [
            "TABLESAMPLE SYSTEM (10)",
            "TABLESAMPLE BERNOULLI (0.5) REPEATABLE (1)",
        ] {
            assert!(parse(&format!("SELECT * FROM t {clause}")).is_ok());
        }
    }

    #[test]
    fn foreign_expression_options_are_rejected_in_nested_contexts() {
        for expr in [
            "sum(x WHERE x>0)",
            "array_agg(x LIMIT 1)",
            "listagg(x ON OVERFLOW ERROR)",
            "f(x ABSENT ON NULL)",
            "sum(x) OVER (GROUPS CURRENT ROW)",
            "a[1:2]",
            "x LIKE ANY ('a','b')",
        ] {
            for sql in [
                format!("SELECT {expr} FROM t"),
                format!("SELECT * FROM (SELECT {expr} FROM t) q"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        assert!(!parse("SELECT sum(x) OVER w FROM t WINDOW w AS (GROUPS CURRENT ROW)").is_ok());
        for expr in [
            "sum(x ORDER BY y)",
            "JSON_ARRAY(x ABSENT ON NULL)",
            "sum(x) OVER (ROWS CURRENT ROW)",
            "sum(x) OVER (RANGE CURRENT ROW)",
            "a[1]",
        ] {
            let sql = format!("SELECT {expr} FROM t");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn rejects_invalid_clauses_in_nested_queries() {
        for query in [
            "SELECT FROM t",
            "SELECT 1 WHERE TRUE",
            "SELECT 1 GROUP BY 1",
            "SELECT 1 HAVING TRUE",
            "SELECT 1 WINDOW w AS ()",
            "SELECT 1 QUALIFY TRUE",
        ] {
            for sql in [query.to_owned(), format!("SELECT * FROM ({query}) q")] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for sql in [
            "SELECT 1",
            "SELECT 1 FROM t WHERE TRUE",
            "SELECT 1 FROM t QUALIFY TRUE",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn validates_types_recursively_in_every_statement_family() {
        for data_type in ["DECIMA(5, 2)", "IN", "ROW(x IN)", "IN ARRAY"] {
            for sql in [
                format!("CREATE TABLE t(x {data_type})"),
                format!("DECLARE RECURSIVE VIEW v(x {data_type})"),
                format!("SELECT CAST(NULL AS {data_type})"),
                format!("CREATE FUNCTION f(x {data_type}) RETURNS INT"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for data_type in [
            "DECIMAL(5, 2)",
            "ANY",
            "my_type",
            "\"IN\" ARRAY",
            "ROW(x my_type)",
        ] {
            let sql = format!("CREATE TABLE t(x {data_type})");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn table_call_lookahead_preserves_explicit_disambiguation() {
        for sql in [
            "SELECT * FROM f(g(1))",
            "SELECT * FROM f(ABS(x))",
            "SELECT * FROM f(g(1)+1)",
            "SELECT * FROM f(\"g\"(1))",
            "SELECT * FROM f(COALESCE(x,1))",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SELECT * FROM f((g(1)))",
            "SELECT * FROM f(1,g(1))",
            "SELECT * FROM f(s.g(1))",
            "SELECT * FROM f(1+g(1))",
            "SELECT * FROM f(ROW(1))",
            "SELECT * FROM f(CAST(x AS INT))",
            "SELECT * FROM f(DATEADD(DAY,1,x))",
            "SELECT * FROM f(x)",
            "SELECT * FROM f(TRIM(x))",
            "SELECT * FROM f(SUBSTRING(x,1))",
            "SELECT * FROM f(EXTRACT(DAY FROM x))",
            "SELECT * FROM f(TUMBLE(x,y))",
            "SELECT * FROM f(HOP(x,y,z))",
            "SELECT * FROM f(SESSION(x,y))",
            "SELECT * FROM f(JSON_ARRAY(1))",
            "SELECT * FROM LATERAL f(g(1))",
            "SELECT * FROM TABLE(f(g(1)))",
            "SELECT * FROM UNNEST(g(1))",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn rejects_foreign_expression_syntax_without_rejecting_neighbors() {
        for sql in [
            "SELECT x:y FROM t",
            "SELECT 1 | 2",
            "SELECT INTERVAL '1'",
            "SELECT INTERVAL '' DAY",
            "SELECT transform(a, b INT -> b+1) FROM t",
            "SELECT IN(1)",
            "SELECT FROM(1)",
            "SELECT x FROM t WHERE x.1=0",
            "INSERT t VALUES (1)",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SELECT x::INT FROM t",
            "SELECT 1 & 2",
            "SELECT 1 ^ 2",
            "SELECT INTERVAL x DAY FROM t",
            "SELECT INTERVAL (x+1) DAY FROM t",
            "SELECT INTERVAL '1' DAY",
            "SELECT transform(a, b -> b+1) FROM t",
            "SELECT \"IN\"(1)",
            "INSERT INTO t VALUES (1)",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }
}
