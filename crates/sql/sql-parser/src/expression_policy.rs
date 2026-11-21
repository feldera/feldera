//! Exhaustive expression-field enumeration plus local Calcite grammar policies.
//!
//! Nested operators, function clauses, identifiers and types have their own
//! guards. This match forces review of new expression variants and fields;
//! it does not declare those nested policies complete.

use sqlparser_064::ast::{
    Array as ArrayLiteral, CeilFloorKind, Expr, ExtractSyntax, InTarget, Value,
};

fn string_literal(expression: &Expr) -> bool {
    matches!(expression,
        Expr::Value(value) if matches!(value.value,
            Value::SingleQuotedString(_) | Value::NationalStringLiteral(_)))
        || matches!(
            expression,
            Expr::StringLiteralChain(_)
                | Expr::Prefixed {
                    prefix: _,
                    value: _
                }
        )
}

pub(super) fn allows(expression: &Expr) -> bool {
    use Expr::*;
    match expression {
        IsNormalized {
            expr: _,
            form: _,
            negated: _,
        } => false,
        IsJson {
            expr: _,
            kind: _,
            unique_keys,
            negated: _,
        } => unique_keys.is_none(),
        In {
            expr: _,
            target,
            negated: _,
            global,
        } => {
            !global
                && match target {
                    InTarget::List(_) | InTarget::Subquery(_) => true,
                    InTarget::Unnest(_) | InTarget::Table { name: _, args: _ } => false,
                }
        }
        RLike {
            negated: _,
            expr: _,
            pattern: _,
            regexp,
        } => !regexp,
        Convert {
            is_try: _,
            expr: _,
            data_type: _,
            charset: _,
            target_before_value: _,
            styles,
        } => styles.is_empty(),
        AtTimeZone {
            timestamp: _,
            time_zone,
        } => string_literal(time_zone),
        Extract {
            field: _,
            syntax,
            expr: _,
        } => match syntax {
            ExtractSyntax::From => true,
            ExtractSyntax::Comma => false,
        },
        Ceil { expr: _, field } | Floor { expr: _, field } => match field {
            CeilFloorKind::DateTimeField(_) => true,
            CeilFloorKind::Scale(_) => false,
        },
        Array(ArrayLiteral { elem: _, named }) => *named,
        // SqlUtil.translateCharacterSetName has this closed set. Other JDK
        // aliases, such as ASCII or ISO8859_1, are not valid SQL introducers.
        Prefixed { prefix, value: _ } => matches!(
            prefix.value.to_ascii_uppercase().as_str(),
            "_BIG5"
                | "_LATIN1"
                | "_UTF8"
                | "_UTF16"
                | "_UTF-16"
                | "_GB2312"
                | "_GBK"
                | "_UTF-16BE"
                | "_UTF-16LE"
                | "_ISO-8859-1"
                | "_UTF-8"
        ),
        Identifier(_)
        | CompoundIdentifier(_)
        | IsFalse(_)
        | IsNotFalse(_)
        | IsTrue(_)
        | IsNotTrue(_)
        | IsNull(_)
        | IsNotNull(_)
        | IsUnknown(_)
        | IsNotUnknown(_)
        | IsDistinctFrom(_, _)
        | IsNotDistinctFrom(_, _)
        | Nested(_)
        | Value(_)
        | StringLiteralChain(_)
        | TypedString(_)
        | Function(_)
        | RowConstructor { fields: _, span: _ }
        | WildcardExclusion {
            wildcard: _,
            columns: _,
            span: _,
        }
        | MapQuery { query: _, span: _ }
        | Subquery(_)
        | GroupingSets(_)
        | Cube(_)
        | Rollup(_)
        | Tuple(_)
        | Dictionary(_)
        | Map(_)
        | QualifiedDateTimeDifference {
            interval: _,
            span: _,
        }
        | Interval(_)
        | Wildcard(_)
        | QualifiedWildcard(_, _)
        | OuterJoin(_)
        | Prior(_)
        | Lambda(_)
        | MemberOf(_) => true,
        CompoundFieldAccess {
            root: _,
            access_chain: _,
        }
        | JsonAccess { value: _, path: _ }
        | Between {
            symmetry: _,
            expr: _,
            negated: _,
            low: _,
            high: _,
        }
        | BinaryOp {
            left: _,
            op: _,
            right: _,
        }
        | Like {
            negated: _,
            any: _,
            expr: _,
            pattern: _,
            escape_char: _,
        }
        | ILike {
            negated: _,
            any: _,
            expr: _,
            pattern: _,
            escape_char: _,
        }
        | SimilarTo {
            negated: _,
            expr: _,
            pattern: _,
            escape_char: _,
        }
        | AnyOp {
            left: _,
            compare_op: _,
            right: _,
            is_some: _,
        }
        | AllOp {
            left: _,
            compare_op: _,
            right: _,
        }
        | UnaryOp { op: _, expr: _ }
        | Cast {
            kind: _,
            expr: _,
            data_type: _,
            format: _,
        }
        | Position { expr: _, r#in: _ }
        | Substring {
            expr: _,
            substring_from: _,
            substring_for: _,
            special: _,
            shorthand: _,
        }
        | Trim {
            trim_where: _,
            trim_what: _,
            expr: _,
            trim_characters: _,
        }
        | Overlay {
            expr: _,
            overlay_what: _,
            overlay_from: _,
            overlay_for: _,
        }
        | Collate {
            expr: _,
            collation: _,
        }
        | Case {
            case_token: _,
            end_token: _,
            operand: _,
            conditions: _,
            else_result: _,
        }
        | Exists {
            subquery: _,
            negated: _,
        }
        | Struct {
            values: _,
            fields: _,
        }
        | Named { expr: _, name: _ }
        | MatchAgainst {
            columns: _,
            match_value: _,
            opt_search_modifier: _,
        } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn outside_grammar_expression_fields_are_rejected_with_positive_controls() {
        for expression in [
            "CONVERT(INT,x,1)",
            "CEIL(x,2)",
            "FLOOR(x,2)",
            "EXTRACT(YEAR,x)",
            "x IS NFC NORMALIZED",
            "x REGEXP y",
            "x AT TIME ZONE y",
            "x IS JSON WITH UNIQUE KEYS",
            "[1,2]",
            "x IN UNNEST(a)",
            "ROLLUP(x,y)",
            "_binary'abc'",
        ] {
            for sql in [
                format!("SELECT {expression} FROM t"),
                format!("SELECT * FROM (SELECT {expression} FROM t) q"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
        for expression in [
            "CONVERT(INT,x)",
            "CEIL(x TO DAY)",
            "FLOOR(x TO DAY)",
            "EXTRACT(YEAR FROM x)",
            "x RLIKE y",
            "x AT TIME ZONE 'UTC'",
            "x IS JSON",
            "ARRAY[1,2]",
            "x IN (1,2)",
            "_utf8'abc'",
            "DATETIME '2020-01-01 00:00:00'",
        ] {
            let sql = format!("SELECT {expression} FROM t");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
        assert!(parse("SELECT x FROM t GROUP BY ROLLUP(x,y)").is_ok());
    }
}
