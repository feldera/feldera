//! Global aggregate components from the S2 input, update and result closures.
use super::{
    Projection, decimal_parameters,
    expression::{Expression, ExpressionKind, indent, print_type, scalar_name},
    literals,
};
use crate::model::{DerivedType, Kind, Outcome, SqlType};
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum AggregateProjection {
    LinearAggregate {
        empty: Projection,
        map: Projection,
        post: Projection,
    },
    MinMaxAggregate {
        empty: Projection,
        zero: Projection,
        increment: Projection,
        semigroup: String,
        compared_value: Projection,
        post: Option<Projection>,
    },
    NonLinearAggregate {
        empty: Projection,
        zero: Projection,
        increment: Projection,
        semigroup: String,
        post: Option<Projection>,
    },
}

const BIGINT: SqlType = SqlType {
    kind: Kind::Integer { bits: 64 },
    nullable: false,
};

fn projection(expression_text: impl Into<String>, type_text: impl Into<String>) -> Projection {
    Projection {
        expression_text: expression_text.into(),
        type_text: type_text.into(),
    }
}

fn scalar(expression: &Expression) -> Projection {
    projection(expression.print(), print_type(expression.ty))
}

fn null(ty: SqlType) -> Projection {
    scalar(&Expression::null(ty))
}

fn tuple(fields: &[Projection], multiline: bool) -> Projection {
    let arity = fields.len();
    let texts = fields.iter().map(|f| f.expression_text.as_str());
    let expression = if multiline {
        let indented: Vec<_> = texts.map(indent).collect();
        format!("Tup{arity}::new(\n{})", indented.join(", \n"))
    } else {
        let texts: Vec<_> = texts.collect();
        format!("Tup{arity}::new({})", texts.join(", "))
    };
    let types: Vec<_> = fields.iter().map(|f| f.type_text.as_str()).collect();
    projection(expression, format!("Tup{arity}<{}>", types.join(", ")))
}

fn if_else(condition: &str, yes: &Projection, no: &Projection) -> Projection {
    projection(
        format!(
            "(if {condition} {{\n{}\n}} else {{\n{}\n}})",
            indent(&yes.expression_text),
            indent(&no.expression_text)
        ),
        &yes.type_text,
    )
}

fn column(input: SqlType, parameter: usize) -> Expression {
    Expression {
        ty: input,
        kind: ExpressionKind::Column {
            variable: parameter,
            ordinal: 0,
        },
    }
}

fn short_name(ty: SqlType) -> String {
    let base = scalar_name(ty.kind);
    if ty.nullable {
        format!("{base}N")
    } else {
        base
    }
}

fn cast_text(expression: &str, from: SqlType, to: SqlType) -> String {
    if from == to {
        expression.to_owned()
    } else {
        let target = super::expression::cast_type(to);
        format!("(({target}){expression})")
    }
}

fn indicator(input: &Expression, result: SqlType, parameter: usize) -> Projection {
    let source = if input.ty.character() {
        input.print()
    } else {
        format!("(*t_{parameter}).0")
    };
    let ty = print_type(result);
    projection(format!("indicator::<_, {ty}>(&{source})"), ty)
}

fn count(input: SqlType, parameter: usize) -> AggregateProjection {
    let one = projection("1i64", "i64");
    let empty = projection("0i64", "i64");
    if input.nullable {
        AggregateProjection::LinearAggregate {
            empty,
            map: tuple(
                &[indicator(&column(input, parameter), BIGINT, parameter), one],
                false,
            ),
            post: projection(format!("t_{}.0", parameter + 1), "i64"),
        }
    } else {
        AggregateProjection::LinearAggregate {
            empty,
            map: one,
            post: projection("t", "i64"),
        }
    }
}

fn minmax(
    operation: &str,
    input: SqlType,
    result: SqlType,
    parameter: usize,
) -> AggregateProjection {
    let empty = null(result);
    let suffix = if input.nullable { "N" } else { "" };
    let (function, semigroup) = if operation == "MIN" {
        ("min", "MinSemigroup")
    } else {
        ("max", "MaxSemigroup")
    };
    let next = parameter + 1;
    let mut accumulator = format!("t_{next}");
    if result.character() {
        accumulator.push_str(".clone()");
    }
    let compared = scalar(&column(input, parameter));
    let increment = projection(
        format!(
            "agg_{function}_N_{suffix}({accumulator}, {})",
            compared.expression_text
        ),
        print_type(result),
    );
    AggregateProjection::MinMaxAggregate {
        zero: empty.clone(),
        empty,
        increment,
        semigroup: format!("{semigroup}<{}>", print_type(result)),
        compared_value: compared,
        post: None,
    }
}

fn array(input: SqlType, nullable: bool, parameter: usize) -> AggregateProjection {
    let name = if nullable { "N" } else { "" };
    let option = if nullable { "?" } else { "" };
    let element = print_type(input);
    let array_type = format!("Array<{element}>{option}");
    let vector_type = format!("Vec<{element}>{option}");
    let wrap = |expression: String| {
        if nullable {
            format!("Some({expression})")
        } else {
            expression
        }
    };
    let value = column(input, parameter).print();
    let accumulator = parameter + 1;
    let post = parameter + 2;
    AggregateProjection::NonLinearAggregate {
        empty: projection(
            wrap(format!("Arc::new(Vec::<{element}>::new())")),
            &array_type,
        ),
        zero: projection(wrap("vec!()".into()), &vector_type),
        increment: projection(
            format!(
                "array_agg{name}(\n    &mut t_{accumulator},\n    {value},\n    t_0,\n    false,\n    true)"
            ),
            "()",
        ),
        semigroup: format!("ConcatSemigroup<{vector_type}>"),
        post: Some(projection(format!("to_array{name}(t_{post})"), array_type)),
    }
}

fn partial_type(result: SqlType) -> SqlType {
    let kind = match result.kind {
        Kind::Integer { bits: 64 } => Kind::Integer { bits: 128 },
        Kind::Integer { .. } => BIGINT.kind,
        Kind::Decimal { precision, scale } if precision <= 12 => Kind::Decimal {
            precision: precision * 3,
            scale: scale * 3,
        },
        Kind::Decimal { precision, scale } => Kind::Decimal {
            precision: 38,
            scale: (u16::from(scale) * 38 / u16::from(precision)) as u8,
        },
        kind => kind,
    };
    result.with_kind(kind)
}

fn numeric_literal(ty: SqlType, value: &str) -> Option<Projection> {
    if ty.kind == (Kind::Integer { bits: 128 }) {
        Some(projection(format!("{value}i128"), "i128"))
    } else {
        Some(scalar(&literals::parse(ty, value)?))
    }
}

fn linear_sum_or_avg(
    operation: &str,
    input: SqlType,
    result: SqlType,
    parameter: usize,
) -> Option<AggregateProjection> {
    let average = operation == "AVG";
    let partial = partial_type(result);
    let nonnull = SqlType {
        nullable: false,
        ..partial
    };
    let value = column(input, parameter);
    let condition = if input.nullable {
        let suffix = if input.character() {
            ".as_ref()"
        } else {
            ".as_ref().cloned()"
        };
        format!("(!(*t_{parameter}).0{suffix}.is_none())")
    } else {
        "(!false)".to_owned()
    };
    let zero = numeric_literal(nonnull, "0")?;
    let one = projection("1i64", "i64");
    let non_null_value = scalar(&value.cast(nonnull));
    let (mapped_value, mapped_count) = if !average && !input.nullable {
        (non_null_value, one.clone())
    } else {
        (
            if_else(&condition, &non_null_value, &zero),
            if_else(&condition, &one, &projection("0i64", "i64")),
        )
    };
    let tail = if average {
        numeric_literal(nonnull, "1")?
    } else {
        one
    };
    let map = tuple(&[mapped_value, mapped_count, tail], true);

    let state = parameter + 1;
    let sum = format!("t_{state}.0");
    let count = format!("t_{state}.1");
    let result_expression = if average {
        let parameters = decimal_parameters(nonnull.kind, nonnull.kind, nonnull.kind);
        let divisor = cast_text(&count, BIGINT, partial);
        let function = format!(
            "div_{}_{}{parameters}",
            short_name(nonnull),
            short_name(partial)
        );
        let division = format!("{function}(\n    {sum},\n    {divisor})");
        cast_text(&division, partial, result)
    } else {
        cast_text(&sum, nonnull, result)
    };
    let post_condition = format!("neq__(\n    {count},\n    0i64)");
    let empty = null(result);
    let post = if_else(
        &post_condition,
        &projection(result_expression, print_type(result)),
        &empty,
    );
    Some(AggregateProjection::LinearAggregate { empty, map, post })
}

fn floating_sum_or_avg(
    operation: &str,
    input: SqlType,
    result: SqlType,
    parameter: usize,
) -> AggregateProjection {
    let next = parameter + 1;
    let post = parameter + 2;
    let value = column(input, parameter).cast(result);
    let base = print_type(SqlType {
        nullable: false,
        ..result
    });
    let opt = print_type(result);
    let short = scalar_name(result.kind);
    let value_text = value.print();
    let empty = null(result);
    let semigroup = format!("DefaultOptSemigroup<{base}>");
    let update_sum =
        |state: &str| format!("agg_plus_{short}N_{short}N({state}, {value_text}.mul_by_ref(&t_0))");
    if operation == "SUM" {
        return AggregateProjection::NonLinearAggregate {
            zero: empty.clone(),
            empty,
            increment: projection(update_sum(&format!("t_{next}")), &opt),
            semigroup,
            post: Some(projection(format!("t_{post}"), opt)),
        };
    }
    let zero = tuple(&[empty.clone(), empty.clone()], false);
    let sum = projection(update_sum(&format!("t_{next}.0.as_ref().cloned()")), &opt);
    let reference = if input.nullable {
        format!("(*t_{parameter}).0")
    } else {
        value_text
    };
    let count = projection(
        format!(
            "agg_plus_{short}N_{short}(t_{next}.1.as_ref().cloned(), indicator::<_, {base}>(&{reference}).mul_by_ref(&t_0))"
        ),
        &opt,
    );
    AggregateProjection::NonLinearAggregate {
        empty,
        zero,
        increment: tuple(&[sum, count], false),
        semigroup: format!("PairSemigroup<{opt}, {opt}, {semigroup}, {semigroup}>"),
        post: Some(projection(
            format!(
                "div_{short}N_{short}N(\n    t_{post}.0.as_ref().cloned(),\n    t_{post}.1.as_ref().cloned())"
            ),
            opt,
        )),
    }
}

pub fn lower_aggregate(
    operation: &str,
    original: &[SqlType],
    values: &[Option<&str>],
    outcome: &Outcome,
) -> Option<Vec<AggregateProjection>> {
    let Outcome::Bound { result, operands } = outcome else {
        return None;
    };
    let [input] = operands.as_slice() else {
        return None;
    };
    let [declared] = original else { return None };
    let [value] = values else { return None };
    let parameter = if value.is_some() || input != declared {
        3
    } else {
        2
    };
    let component = match (operation, result) {
        ("COUNT", DerivedType::Scalar(_)) => count(*input, parameter),
        ("MIN" | "MAX", DerivedType::Scalar(result)) => {
            minmax(operation, *input, *result, parameter)
        }
        ("ARRAY_AGG", DerivedType::Array { nullable, .. }) => array(*input, *nullable, parameter),
        ("SUM" | "AVG", DerivedType::Scalar(result)) => {
            if matches!(result.kind, Kind::Real | Kind::Double) {
                floating_sum_or_avg(operation, *input, *result, parameter)
            } else {
                linear_sum_or_avg(operation, *input, *result, parameter)?
            }
        }
        _ => return None,
    };
    Some(vec![component])
}
