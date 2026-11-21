//! Unoptimized scalar projection lowering for the generated scalar probes.
//!
//! The caller supplies the bound types from either Rust inference strategy.
//! This module models the later conversions in ExpressionCompiler.java and
//! prints the resulting expression in the Java inner printer's compact form.

mod aggregates;
mod expression;
mod literals;
mod sets;

use crate::model::{DerivedType, Kind, Outcome, SqlType};
use expression::{Expression, ExpressionKind, print_type, scalar_name};
use literals::Literal;
use serde::Serialize;
pub use sets::{SetProjection, lower_set_projections};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Projection {
    pub expression_text: String,
    pub type_text: String,
}

pub use aggregates::{AggregateProjection, lower_aggregate};

enum Suffix {
    Type,
    Nullable,
    None,
}

fn call(
    base: &str,
    suffix: Suffix,
    trailing: &str,
    ty: SqlType,
    arguments: Vec<Expression>,
) -> Expression {
    let mut function = base.to_owned();
    for argument in &arguments {
        match suffix {
            Suffix::Type => {
                function.push('_');
                function.push_str(&scalar_name(argument.ty.kind));
                if argument.ty.nullable {
                    function.push('N');
                }
            }
            Suffix::Nullable => function.push(if argument.ty.nullable { 'N' } else { '_' }),
            Suffix::None => {}
        }
    }
    function.push_str(trailing);
    Expression {
        ty,
        kind: ExpressionKind::Call {
            function,
            arguments,
        },
    }
}

fn lower_strings(
    operation: &str,
    result: SqlType,
    mut arguments: Vec<Expression>,
) -> Option<Expression> {
    let (base, suffix) = match operation {
        "LENGTH" | "CHAR_LENGTH" => ("char_length", Suffix::Nullable),
        "OCTET_LENGTH" => ("octet_length", Suffix::Nullable),
        "UPPER" => ("upper", Suffix::Nullable),
        "LOWER" => ("lower", Suffix::Nullable),
        "INITCAP" => ("initcap", Suffix::Nullable),
        "REVERSE" => ("reverse", Suffix::Nullable),
        "ASCII" => ("ascii", Suffix::Nullable),
        "MD5" => ("md5", Suffix::Type),
        "TRIM" => {
            arguments.insert(
                0,
                Expression {
                    ty: SqlType {
                        kind: Kind::Character {
                            fixed: true,
                            length: Some(1),
                        },
                        nullable: false,
                    },
                    kind: ExpressionKind::TrimSpace,
                },
            );
            ("trim_both", Suffix::Type)
        }
        "CONCAT" => {
            let source = call("concat", Suffix::Type, "", result, arguments);
            return Some(Expression {
                ty: result,
                kind: ExpressionKind::Clone(Box::new(source)),
            });
        }
        "LEFT" | "RIGHT" | "REPEAT" => {
            let right = arguments.pop()?.convert_to(Kind::Integer { bits: 32 });
            arguments.push(right);
            match operation {
                "LEFT" => ("left", Suffix::Type),
                "RIGHT" => ("right", Suffix::Type),
                _ => ("repeat", Suffix::Nullable),
            }
        }
        _ => return None,
    };
    Some(call(base, suffix, "", result, arguments))
}

fn integer_precision(bits: u8) -> Option<u8> {
    Some(match bits {
        8 => 3,
        16 => 5,
        32 => 10,
        64 => 19,
        _ => return None,
    })
}

fn lower_division(
    function: &str,
    result: SqlType,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    let nullable = left.ty.nullable || right.ty.nullable;
    let (left, right, call_kind) = match (left.ty.kind, right.ty.kind) {
        (Kind::Integer { bits: l }, Kind::Integer { bits: r }) => {
            let kind = Kind::Integer { bits: l.max(r) };
            (left.convert_to(kind), right.convert_to(kind), kind)
        }
        (Kind::Decimal { .. }, Kind::Decimal { .. }) => (left, right, result.kind),
        (Kind::Decimal { precision, scale }, Kind::Integer { bits })
        | (Kind::Integer { bits }, Kind::Decimal { precision, scale }) => {
            let common = Kind::Decimal {
                precision: precision.max(integer_precision(bits)?),
                scale,
            };
            (
                left.convert_to(common),
                right.convert_to(common),
                Kind::Decimal {
                    precision: 38,
                    scale: 10,
                },
            )
        }
        (l, r)
            if matches!(l, Kind::Real | Kind::Double) || matches!(r, Kind::Real | Kind::Double) =>
        {
            (
                left.convert_to(result.kind),
                right.convert_to(result.kind),
                result.kind,
            )
        }
        _ => return None,
    };
    let type_parameters = decimal_parameters(left.ty.kind, right.ty.kind, call_kind);
    let call_type = SqlType {
        kind: call_kind,
        nullable,
    };
    Some(
        call(
            function,
            Suffix::Type,
            &type_parameters,
            call_type,
            vec![left, right],
        )
        .cast(result),
    )
}

fn decimal_parameters(left: Kind, right: Kind, result: Kind) -> String {
    match (left, right, result) {
        (
            Kind::Decimal {
                precision: lp,
                scale: ls,
            },
            Kind::Decimal {
                precision: rp,
                scale: rs,
            },
            Kind::Decimal { precision, scale },
        ) => format!("::<{lp}, {ls}, {rp}, {rs}, {precision}, {scale}>"),
        _ => String::new(),
    }
}

fn lower_coalesce(result: SqlType, arguments: Vec<Expression>) -> Option<Expression> {
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    if !left.ty.nullable {
        let source = left.cast(result);
        let needs_clone = result.character() && matches!(source.kind, ExpressionKind::Cast(_));
        return Some(if needs_clone {
            Expression {
                ty: result,
                kind: ExpressionKind::Clone(Box::new(source)),
            }
        } else {
            source
        });
    }
    let nullable = SqlType {
        nullable: true,
        ..result
    };
    let expression = call(
        "coalesce_N_N!",
        Suffix::None,
        "",
        nullable,
        vec![left.cast(nullable), right.cast(nullable)],
    );
    Some(if result.nullable {
        expression
    } else {
        Expression {
            ty: result,
            kind: ExpressionKind::Unwrap(Box::new(expression)),
        }
    })
}

fn comparison_kind(left: Kind, right: Kind) -> Option<Kind> {
    use Kind::*;
    match (left, right) {
        (l, r) if l == r => Some(l),
        (Character { .. }, Character { .. }) | (Decimal { .. }, Decimal { .. }) => None,
        (Decimal { .. }, Character { .. }) | (Character { .. }, Decimal { .. }) => Some(Decimal {
            precision: 38,
            scale: 19,
        }),
        (Character { .. }, other) | (other, Character { .. }) => Some(other),
        (Integer { bits: left }, Integer { bits: right }) => Some(Integer {
            bits: left.max(right),
        }),
        (Integer { bits }, Decimal { precision, scale })
        | (Decimal { precision, scale }, Integer { bits }) => Some(Decimal {
            precision: precision.max(integer_precision(bits)?),
            scale,
        }),
        (Double, _) | (_, Double) | (Real, Decimal { .. }) | (Decimal { .. }, Real) => Some(Double),
        (Real, Integer { .. }) | (Integer { .. }, Real) => Some(Real),
        (date @ Timestamp { .. }, Date) | (Date, date @ Timestamp { .. }) => Some(date),
        _ => None,
    }
}

fn lower_nullif(result: SqlType, arguments: Vec<Expression>) -> Option<Expression> {
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    let otherwise = Box::new(left.clone().cast(result));
    let any_null_literal = left.is_null_literal() || right.is_null_literal();
    let (left, right) = match comparison_kind(left.ty.kind, right.ty.kind) {
        Some(kind) => (left.convert_to(kind), right.convert_to(kind)),
        None => (left, right),
    };
    let eq_type = SqlType {
        kind: Kind::Boolean,
        nullable: left.ty.nullable || right.ty.nullable,
    };
    let eq = if any_null_literal {
        Expression::null(eq_type)
    } else {
        comparison_call("eq", eq_type, left, right)
    };
    let condition = if eq_type.nullable {
        call(
            "wrap_bool",
            Suffix::None,
            "",
            SqlType {
                nullable: false,
                ..eq_type
            },
            vec![eq],
        )
    } else {
        eq
    };
    Some(Expression {
        ty: result,
        kind: ExpressionKind::If {
            condition: Box::new(condition),
            positive: Box::new(Expression::null(result)),
            negative: otherwise,
        },
    })
}

fn comparison_call(base: &str, result: SqlType, left: Expression, right: Expression) -> Expression {
    let function = format!(
        "{base}_{}_{}",
        if left.ty.nullable { "N" } else { "" },
        if right.ty.nullable { "N" } else { "" },
    );
    call(&function, Suffix::None, "", result, vec![left, right])
}

fn lower_comparison(
    operation: &str,
    result: SqlType,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    use Kind::*;
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    let exact = |kind| matches!(kind, Integer { .. } | Decimal { .. });
    let kind = match (left.ty.kind, right.ty.kind) {
        (Boolean, kind) | (kind, Boolean) if exact(kind) || matches!(kind, Real | Double) => kind,
        (Character { .. }, Character { .. }) => crate::rewritten::merge(left.ty, right.ty)?,
        (Decimal { .. }, Character { .. }) | (Character { .. }, Decimal { .. }) => Decimal {
            precision: 38,
            scale: 19,
        },
        (Character { .. }, other) | (other, Character { .. }) => other,
        (Double, _) | (_, Double) => Double,
        (Real, _) | (_, Real) => Real,
        _ => crate::rewritten::merge(left.ty, right.ty)?,
    };
    let align = |arg: Expression| {
        let target = arg.ty.with_kind(kind);
        if arg.ty.kind == Boolean && exact(kind) {
            boolean_to_exact(arg, target)
        } else {
            case_arm(arg, target)
        }
    };
    let left = align(left)?;
    let right = align(right)?;
    if left.is_null_literal() || right.is_null_literal() {
        return Some(Expression::null(result));
    }
    let function = if operation == "=" { "eq" } else { "lt" };
    Some(comparison_call(function, result, left, right))
}

fn boolean_to_exact(argument: Expression, target: SqlType) -> Option<Expression> {
    let boolean = SqlType {
        kind: Kind::Boolean,
        nullable: false,
    };
    let nonnull = SqlType {
        nullable: false,
        ..target
    };
    let condition = if argument.ty.nullable {
        call(
            "wrap_bool",
            Suffix::None,
            "",
            boolean,
            vec![argument.clone()],
        )
    } else {
        argument.clone()
    };
    let expression = Expression {
        ty: nonnull,
        kind: ExpressionKind::If {
            condition: Box::new(condition),
            positive: Box::new(literals::parse(nonnull, "1")?),
            negative: Box::new(literals::parse(nonnull, "0")?),
        },
    };
    if target.nullable {
        let is_null = Expression {
            ty: boolean,
            kind: ExpressionKind::IsNull(Box::new(argument)),
        };
        let condition = Expression {
            ty: boolean,
            kind: ExpressionKind::Not(Box::new(is_null)),
        };
        Some(Expression {
            ty: target,
            kind: ExpressionKind::If {
                condition: Box::new(condition),
                positive: Box::new(expression.cast(target)),
                negative: Box::new(Expression::null(target)),
            },
        })
    } else {
        Some(expression)
    }
}

fn lower_arithmetic(
    operation: &str,
    result: SqlType,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    if operation == "/" {
        return lower_division("div", result, arguments);
    }
    let function = match operation {
        "+" => "plus",
        "-" => "minus",
        "*" => "times",
        _ => return None,
    };
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    let (left, right) = if matches!(left.ty.kind, Kind::Decimal { .. })
        && matches!(right.ty.kind, Kind::Decimal { .. })
    {
        (left, right)
    } else {
        (left.convert_to(result.kind), right.convert_to(result.kind))
    };
    let type_parameters = decimal_parameters(left.ty.kind, right.ty.kind, result.kind);
    Some(call(
        function,
        Suffix::Type,
        &type_parameters,
        result,
        vec![left, right],
    ))
}

fn lower_case(
    predicate_input: SqlType,
    result: SqlType,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    let [left, right]: [Expression; 2] = arguments.try_into().ok()?;
    let boolean = SqlType {
        kind: Kind::Boolean,
        nullable: false,
    };
    let condition = if predicate_input.nullable {
        Expression {
            ty: boolean,
            kind: ExpressionKind::IsNull(Box::new(Expression {
                ty: predicate_input,
                kind: ExpressionKind::Column {
                    variable: 1,
                    ordinal: 0,
                },
            })),
        }
    } else {
        Expression {
            ty: boolean,
            kind: ExpressionKind::Literal(Literal::Boolean(false)),
        }
    };
    Some(Expression {
        ty: result,
        kind: ExpressionKind::If {
            condition: Box::new(condition),
            positive: Box::new(case_arm(left, result)?),
            negative: Box::new(case_arm(right, result)?),
        },
    })
}

fn case_arm(mut expression: Expression, result: SqlType) -> Option<Expression> {
    if expression.ty == result || expression.is_null_literal() {
        return Some(expression.validator_cast(result));
    }
    let nonnull = SqlType {
        nullable: false,
        ..result
    };
    match (&expression.kind, result.kind) {
        (
            ExpressionKind::Literal(Literal::Integer(value)),
            Kind::Integer { .. } | Kind::Decimal { .. },
        ) => {
            expression = literals::parse(nonnull, &value.to_string())?;
        }
        (
            ExpressionKind::Literal(Literal::Decimal {
                value,
                scale: literal_scale,
                ..
            }),
            Kind::Decimal { precision, scale },
        ) => {
            let whole = value
                .split('.')
                .next()?
                .trim_start_matches('-')
                .trim_start_matches('0');
            let available = usize::from(precision - scale);
            if whole.len() <= available && *literal_scale <= usize::from(scale) {
                expression = literals::parse(nonnull, value)?;
            }
        }
        (ExpressionKind::Literal(Literal::String(_)), Kind::Character { .. }) => {
            expression.ty.kind = result.kind;
        }
        _ if result.character() && !expression.ty.character()
            || matches!(
                (expression.ty.kind, result.kind),
                (Kind::Date, Kind::Timestamp { .. })
            ) =>
        {
            let target = expression.ty.with_kind(result.kind);
            expression = expression.validator_cast(target);
        }
        _ => {}
    }
    Some(expression.validator_cast(result))
}

enum NumericRule {
    Preserve,
    IntegralNoOp,
    Round,
    DoubleArguments,
    Power,
}

fn rule(operation: &str) -> Option<NumericRule> {
    use NumericRule::*;
    Some(match operation {
        "ABS" | "SIGN" => Preserve,
        "CEIL" | "FLOOR" => IntegralNoOp,
        "ROUND" | "TRUNCATE" => Round,
        "SQRT" | "CBRT" | "EXP" | "LN" | "LOG10" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS"
        | "ATAN" | "COT" | "DEGREES" | "RADIANS" | "LOG" | "ATAN2" => DoubleArguments,
        "POWER" => Power,
        _ => return None,
    })
}

fn normalize(rule: NumericRule, mut arguments: Vec<Expression>) -> Vec<Expression> {
    match rule {
        NumericRule::DoubleArguments => {
            arguments = arguments
                .into_iter()
                .map(|arg| arg.convert_to(Kind::Double))
                .collect();
        }
        NumericRule::Power => {
            arguments = arguments
                .into_iter()
                .enumerate()
                .map(|(index, arg)| {
                    let kind = match (index, arg.ty.kind) {
                        (1, Kind::Integer { .. }) => Kind::Integer { bits: 32 },
                        (_, Kind::Integer { .. } | Kind::Real | Kind::Decimal { .. }) => {
                            Kind::Double
                        }
                        (_, kind) => kind,
                    };
                    arg.convert_to(kind)
                })
                .collect();
        }
        NumericRule::Round => arguments.push(Expression {
            ty: SqlType {
                kind: Kind::Integer { bits: 32 },
                nullable: false,
            },
            kind: ExpressionKind::Zero,
        }),
        _ => {}
    }
    arguments
}

pub fn lower_projection(
    operation: &str,
    declared: &[SqlType],
    literal_values: &[Option<&str>],
    outcome: &Outcome,
) -> Option<Projection> {
    let Outcome::Bound {
        result: DerivedType::Scalar(result),
        operands,
    } = outcome
    else {
        return None;
    };
    let arity = match operation {
        "+" | "-" | "*" | "/" | "CASE" | "=" | "<" => Some(2),
        _ => crate::sampled_arity(operation),
    };
    if arity != Some(declared.len())
        || operands.len() != declared.len()
        || literal_values.len() != declared.len()
        || !result.is_valid()
        || operands.iter().any(|ty| !ty.is_valid())
        || declared.iter().any(|ty| !ty.is_valid())
    {
        return None;
    }
    let arguments = declared
        .iter()
        .zip(operands)
        .zip(literal_values)
        .enumerate()
        .map(|(index, ((&ty, &target), value))| {
            let expression = match value {
                Some(text) => literals::parse(ty, text)?,
                None => Expression {
                    ty,
                    kind: ExpressionKind::Column {
                        variable: 1,
                        ordinal: index,
                    },
                },
            };
            Some(if operation == "CASE" {
                expression
            } else {
                expression.validator_cast(target)
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let expression =
        if null_short_circuit(operation) && arguments.iter().any(Expression::is_null_literal) {
            Expression::null(*result)
        } else {
            match rule(operation) {
                Some(rule) if result.numeric() && operands.iter().all(|ty| ty.numeric()) => {
                    lower_numeric(operation, rule, *result, arguments)?
                }
                Some(_) => return None,
                None if operation == "MOD" => lower_division("modulo", *result, arguments)?,
                None if operation == "COALESCE" => lower_coalesce(*result, arguments)?,
                None if operation == "NULLIF" => lower_nullif(*result, arguments)?,
                None if operation == "CASE" => lower_case(declared[0], *result, arguments)?,
                None if matches!(operation, "=" | "<") => {
                    lower_comparison(operation, *result, arguments)?
                }
                None if matches!(operation, "+" | "-" | "*" | "/") => {
                    lower_arithmetic(operation, *result, arguments)?
                }
                None => lower_strings(operation, *result, arguments)?,
            }
        };
    Some(Projection {
        expression_text: expression.print(),
        type_text: print_type(expression.ty),
    })
}

fn null_short_circuit(operation: &str) -> bool {
    matches!(
        operation,
        "+" | "-"
            | "*"
            | "/"
            | "SIGN"
            | "CEIL"
            | "FLOOR"
            | "CBRT"
            | "EXP"
            | "LN"
            | "LOG10"
            | "SIN"
            | "COS"
            | "TAN"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "COT"
            | "DEGREES"
            | "RADIANS"
            | "LOG"
            | "ATAN2"
            | "POWER"
            | "MOD"
            | "OCTET_LENGTH"
            | "UPPER"
            | "LOWER"
            | "INITCAP"
            | "ASCII"
            | "MD5"
    )
}

fn lower_numeric(
    operation: &str,
    rule: NumericRule,
    result: SqlType,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    if matches!(rule, NumericRule::IntegralNoOp | NumericRule::Round)
        && matches!(arguments.first()?.ty.kind, Kind::Integer { .. })
    {
        return arguments.into_iter().next();
    }
    let arguments = normalize(rule, arguments);
    let nullable_argument = arguments.iter().any(|arg| arg.ty.nullable);
    let call_type = SqlType {
        nullable: result.nullable && nullable_argument,
        ..result
    };
    let call = call(
        &operation.to_ascii_lowercase(),
        Suffix::Type,
        "",
        call_type,
        arguments,
    );
    Some(
        if matches!(operation, "LN" | "LOG10") && !nullable_argument {
            call
        } else {
            call.cast(result)
        },
    )
}
