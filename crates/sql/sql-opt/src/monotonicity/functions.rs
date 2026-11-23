//! Select the runtime function that computes a lower bound for a scalar call.
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field};
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;

/// Build the selected runtime call in the original call's location. A bounded
/// operand retains its reduced expression. An unbounded constant needs its own
/// copy because the source call can also survive in the circuit.
pub fn build_function_waterline(
    edit: &mut CircuitEditor,
    call: ExpressionId,
    target: &str,
    bounds: &[Option<ExpressionId>],
) -> Result<ExpressionId, Error> {
    let site = call.into();
    let arguments = edit.node(site)?.edges("arguments")?;
    if arguments.len() != bounds.len() {
        return Err(Error::new("expected one waterline slot per call argument"));
    }
    let mut operands = Vec::with_capacity(bounds.len());
    for (original, bound) in arguments.into_iter().zip(bounds) {
        let operand = match bound {
            Some(bound) => (*bound).into(),
            None => copy_expression(edit, original)?,
        };
        operands.push(Field::Reference(operand));
    }
    let callee = edit.function_path(site, target)?;
    let required = edit.with_nullability(edit.expression_type(call)?, false)?;
    edit.make_expr(
        site,
        Expr::Apply,
        required,
        [
            ("function", Field::Reference(callee.into())),
            ("arguments", Field::Array(operands)),
        ],
    )
}

/// Java writes a DOUBLE literal's raw IEEE 754 bits as a signed long.
pub(crate) fn double_literal_above_one(
    edit: &CircuitEditor,
    id: ExpressionId,
) -> Result<bool, Error> {
    if edit.expr_kind(id)? != Expr::DoubleLiteral {
        return Ok(false);
    }
    let Some(value) = edit.node(id.into())?.data("value").filter(|v| !v.is_null()) else {
        return Ok(false);
    };
    let bits = value
        .as_i64()
        .ok_or_else(|| Error::new("expected signed IEEE 754 bits in DOUBLE literal"))?;
    Ok(f64::from_bits(bits as u64) > 1.0)
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Argument {
    pub monotone: bool,
    pub constant: bool,
    pub double_literal_above_one: bool,
}

#[derive(Clone, Copy)]
enum Arguments {
    Unary,
    ConstantOthers(usize),
    ConstantBase { value: usize, base: usize },
}

impl Arguments {
    fn admits(self, args: &[Argument]) -> bool {
        match self {
            Self::Unary => args.first().is_some_and(|a| a.monotone),
            Self::ConstantOthers(value) => {
                args.get(value).is_some_and(|a| a.monotone)
                    && args
                        .iter()
                        .enumerate()
                        .all(|(i, a)| i == value || a.constant)
            }
            Self::ConstantBase { value, base } => {
                args.get(value).is_some_and(|a| a.monotone)
                    && args
                        .get(base)
                        .is_some_and(|a| a.constant && a.double_literal_above_one)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Runtime {
    Direct,
    Saturating,
}

struct Function {
    names: Regex,
    arguments: Arguments,
    runtime: Runtime,
}

// # marks the optional nullable suffix on the monotone argument. Other
// argument suffixes are significant and survive in the runtime call name.
static FUNCTIONS: LazyLock<Vec<Function>> = LazyLock::new(|| {
    use Arguments::*;
    use Runtime::*;
    let rules = [
        ("(ln|log10|log|sqrt)_d#", Unary, Saturating),
        ("log_d#_d", ConstantBase { value: 0, base: 1 }, Saturating),
        ("power_d_(d|i32)#", ConstantBase { value: 1, base: 0 }, Direct),
        ("(cbrt|exp|atan|sinh|asinh|tanh|degrees|radians)_d#", Unary, Direct),
        ("sign_(d|f|SqlDecimal|i8|i16|i32|i64|i128|u8|u16|u32|u64|u128)#", Unary, Direct),
        ("(floor|ceil)_(d|f|SqlDecimal)#", Unary, Direct),
        ("floor_(millennium|century|decade|week)_(Date|Timestamp|TimestampTz)#", Unary, Saturating),
        ("floor_(year|quarter|month|day|hour|minute|second|millisecond|microsecond|nanosecond)_(Date|Timestamp|TimestampTz)#", Unary, Direct),
        ("floor_(millennium|century|decade|year|quarter|month|week|day|hour|minute|second|millisecond|microsecond|nanosecond)_Time#", Unary, Direct),
        ("ceil_(millennium|century|decade|year|quarter|month|week)_(Date|Timestamp|TimestampTz)#", Unary, Saturating),
        ("ceil_(day|hour|minute|second|millisecond)_(Timestamp|TimestampTz)#", Unary, Saturating),
        ("ceil_(microsecond|nanosecond)_(Timestamp|TimestampTz)#", Unary, Direct),
        ("ceil_(day|hour|minute|second|millisecond|microsecond|nanosecond)_Date#", Unary, Direct),
        ("date_trunc_(decade|week)_(Date|Timestamp|TimestampTz)#", Unary, Saturating),
        ("date_trunc_(millennium|century|year|quarter|month|day)_(Date|Timestamp|TimestampTz)#", Unary, Direct),
        ("timestamp_trunc_(decade|week)_(Timestamp|TimestampTz)#", Unary, Saturating),
        ("timestamp_trunc_(millennium|century|year|quarter|month|day|hour|minute|second)_(Timestamp|TimestampTz)#", Unary, Direct),
        ("time_trunc_(hour|minute|second|millisecond|microsecond|nanosecond)_Time#", Unary, Direct),
        ("extract_(year|millennium|century|decade)_(Date|Timestamp|TimestampTz|LongInterval)#", Unary, Direct),
        ("extract_isoyear_(Date|Timestamp|TimestampTz)#", Unary, Direct),
        ("extract_epoch_(Date|Timestamp|TimestampTz|ShortInterval)#", Unary, Direct),
        ("extract_epoch_LongInterval#", Unary, Saturating),
        ("extract_hour_Time#", Unary, Direct),
        ("to_bound_ShortInterval_(ShortInterval|Date|Timestamp|Time)_(u64|u128)", Unary, Direct),
        ("(round|trunc)_SqlDecimal#", Unary, Direct),
        ("(round|bround)_SqlDecimal#_i32", ConstantOthers(0), Saturating),
        ("(truncate|trunc)_SqlDecimal#_i32", ConstantOthers(0), Direct),
        ("(round|truncate|trunc)_(d|f)#_i32", ConstantOthers(0), Direct),
        ("tumble_(Timestamp|TimestampTz)#_ShortInterval(_Time|_ShortInterval)?", ConstantOthers(0), Saturating),
        ("hop_start_timestamp", ConstantOthers(0), Direct),
        ("datediff_(day|hour|week|month|quarter|year)_Date_Date#", ConstantOthers(1), Direct),
    ];
    rules
        .into_iter()
        .map(|(pattern, arguments, runtime)| {
            let pattern = pattern.replace('#', "(?P<nullable>N?)");
            let names = Regex::new(&format!(r"\A(?:{pattern})\z"))
                .expect("valid monotone function pattern");
            Function {
                names,
                arguments,
                runtime,
            }
        })
        .collect()
});

/// Return the non-null runtime callee for a lower bound, if the operands admit one.
pub fn function_waterline(name: &str, args: &[Argument]) -> Option<String> {
    for function in FUNCTIONS.iter() {
        let Some(matched) = function.names.captures(name) else {
            continue;
        };
        if !function.arguments.admits(args) {
            return None;
        }
        let mut callee = name.to_owned();
        if let Some(suffix) = matched.name("nullable") {
            callee.replace_range(suffix.range(), "");
        }
        if matches!(function.runtime, Runtime::Saturating) {
            callee.push_str("_saturating");
        }
        return Some(callee);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, args: &[Argument]) -> Option<String> {
        function_waterline(name, args)
    }

    #[test]
    fn temporal_rounding_and_nullable_suffixes() {
        let bound = Argument {
            monotone: true,
            ..Argument::default()
        };
        let literal = Argument {
            constant: true,
            ..Argument::default()
        };
        for (name, expected) in [
            ("floor_week_DateN", Some("floor_week_Date_saturating")),
            ("floor_year_TimestampTzN", Some("floor_year_TimestampTz")),
            (
                "ceil_microsecond_TimestampN",
                Some("ceil_microsecond_Timestamp"),
            ),
            ("ceil_hour_TimeN", None),
            ("extract_hour_Timestamp", None),
            (
                "extract_epoch_LongIntervalN",
                Some("extract_epoch_LongInterval_saturating"),
            ),
            ("floor_year_TimestampNN", None),
        ] {
            assert_eq!(call(name, &[bound]).as_deref(), expected, "{name}");
        }
        assert_eq!(
            call(
                "tumble_TimestampN_ShortInterval_Time",
                &[bound, literal, literal]
            )
            .as_deref(),
            Some("tumble_Timestamp_ShortInterval_Time_saturating")
        );
        assert_eq!(
            call(
                "tumble_TimestampN_ShortInterval_Time",
                &[bound, literal, bound]
            ),
            None
        );
    }

    #[test]
    fn logarithm_power_and_datediff_use_different_argument_positions() {
        let bound = Argument {
            monotone: true,
            ..Argument::default()
        };
        let one = Argument {
            constant: true,
            ..Argument::default()
        };
        let two = Argument {
            double_literal_above_one: true,
            ..one
        };
        assert_eq!(
            call("log_dN_d", &[bound, two]).as_deref(),
            Some("log_d_d_saturating")
        );
        assert_eq!(call("log_dN_d", &[bound, one]), None);
        assert_eq!(
            call("power_d_i32N", &[two, bound]).as_deref(),
            Some("power_d_i32")
        );
        assert_eq!(call("power_d_i32N", &[bound, two]), None);
        assert_eq!(
            call("datediff_day_Date_DateN", &[one, bound]).as_deref(),
            Some("datediff_day_Date_Date")
        );
        assert_eq!(call("datediff_day_Date_DateN", &[bound, one]), None);
    }
}
