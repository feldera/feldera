//! Literal decoding for the generated type probe grammar.
use super::expression::{Expression, ExpressionKind, scalar_name};
use crate::model::{Kind, SqlType};

#[derive(Clone)]
pub(super) enum Literal {
    Boolean(bool),
    Integer(i64),
    Decimal {
        value: String,
        unscaled: i128,
        scale: usize,
    },
    String(String),
    Date {
        days: i32,
        iso: String,
    },
    Timestamp {
        micros: i64,
        iso: String,
    },
}

impl Literal {
    pub(super) fn print(&self, ty: SqlType) -> String {
        match self {
            Literal::Boolean(value) => value.to_string(),
            Literal::Integer(value) => format!("{value}{}", scalar_name(ty.kind)),
            Literal::String(value) => format!("SqlString::from(arcstr::literal!({value:?}))"),
            Literal::Date { days, iso } => format!("Date::from_days({days}/*{iso}*/)"),
            Literal::Timestamp { micros, iso } => {
                format!("Timestamp::from_microseconds({micros}/*{iso}*/)")
            }
            Literal::Decimal {
                value,
                unscaled,
                scale,
            } => {
                let Kind::Decimal {
                    precision,
                    scale: target_scale,
                } = ty.kind
                else {
                    unreachable!("decimal literal requires a decimal type");
                };
                format!(
                    "SqlDecimal::<{precision}, {target_scale}>::new({unscaled}i128, {scale}).expect(\"Could not convert {value} to DECIMAL({precision}, {target_scale})\")"
                )
            }
        }
    }
}

fn decimal_literal(text: &str, declared: SqlType) -> Option<Expression> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let fraction = fraction.trim_end_matches('0');
    let whole_digits = whole.trim_start_matches('-');
    if whole_digits.is_empty()
        || !whole_digits.bytes().all(|c| c.is_ascii_digit())
        || !fraction.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let unscaled = format!("{whole}{fraction}").parse().ok()?;
    let value = if fraction.is_empty() {
        whole.to_owned()
    } else {
        format!("{whole}.{fraction}")
    };
    let natural_precision = u8::try_from(whole_digits.len() + fraction.len()).ok()?;
    let kind = match declared.kind {
        Kind::Decimal { .. } => declared.kind,
        Kind::Real | Kind::Double => Kind::Decimal {
            precision: natural_precision,
            scale: u8::try_from(fraction.len()).ok()?,
        },
        _ => return None,
    };
    let ty = declared.with_kind(kind);
    if !ty.is_valid() {
        return None;
    }
    Some(
        Expression {
            ty,
            kind: ExpressionKind::Literal(Literal::Decimal {
                value,
                unscaled,
                scale: fraction.len(),
            }),
        }
        .cast(declared),
    )
}

pub(super) fn parse(ty: SqlType, text: &str) -> Option<Expression> {
    if ty.nullable {
        return Some(Expression::null(ty));
    }
    let value = match ty.kind {
        Kind::Boolean => Literal::Boolean(match text {
            "TRUE" => true,
            "FALSE" => false,
            _ => return None,
        }),
        Kind::Integer { bits } => {
            let value = text.parse::<i64>().ok()?;
            match bits {
                8 if i8::try_from(value).is_ok() => {}
                16 if i16::try_from(value).is_ok() => {}
                32 if i32::try_from(value).is_ok() => {}
                64 => {}
                _ => return None,
            }
            Literal::Integer(value)
        }
        Kind::Real | Kind::Double | Kind::Decimal { .. } => return decimal_literal(text, ty),
        Kind::Character { fixed, length } => {
            let value = text.strip_prefix('\'')?.strip_suffix('\'')?;
            if !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b' ' | b'-'))
            {
                return None;
            }
            let size = u32::try_from(value.len()).ok()?;
            if fixed && length != Some(size) {
                return Some(
                    Expression {
                        ty: ty.with_kind(Kind::Character {
                            fixed: true,
                            length: Some(size),
                        }),
                        kind: ExpressionKind::Literal(Literal::String(value.into())),
                    }
                    .cast(ty),
                );
            }
            Literal::String(value.into())
        }
        Kind::Date => {
            let iso = text.strip_prefix("DATE '")?.strip_suffix('\'')?;
            Literal::Date {
                days: epoch_days(iso)?,
                iso: iso.into(),
            }
        }
        Kind::Timestamp { precision: 6 } => {
            let iso = text.strip_prefix("TIMESTAMP '")?.strip_suffix('\'')?;
            let (date, time) = iso.split_once(' ')?;
            let time = time.as_bytes();
            let [h0, h1, b':', m0, m1, b':', s0, s1] = time else {
                return None;
            };
            let hours = two_digits(*h0, *h1)?;
            let minutes = two_digits(*m0, *m1)?;
            let seconds = two_digits(*s0, *s1)?;
            if hours >= 24 || minutes >= 60 || seconds >= 60 {
                return None;
            }
            let micros = (i64::from(epoch_days(date)?) * 86_400
                + i64::from(hours * 3600 + minutes * 60 + seconds))
                * 1_000_000;
            Literal::Timestamp {
                micros,
                iso: iso.into(),
            }
        }
        _ => return None,
    };
    Some(Expression {
        ty,
        kind: ExpressionKind::Literal(value),
    })
}

fn two_digits(a: u8, b: u8) -> Option<i32> {
    (a.is_ascii_digit() && b.is_ascii_digit())
        .then(|| i32::from(a - b'0') * 10 + i32::from(b - b'0'))
}

fn epoch_days(iso: &str) -> Option<i32> {
    let [y0, y1, y2, y3, b'-', m0, m1, b'-', d0, d1] = iso.as_bytes() else {
        return None;
    };
    let year = two_digits(*y0, *y1)? * 100 + two_digits(*y2, *y3)?;
    let month = usize::try_from(two_digits(*m0, *m1)?).ok()?;
    let day = two_digits(*d0, *d1)?;
    if year == 0 || !(1..=12).contains(&month) || day == 0 {
        return None;
    }
    let leap = |year: i32| year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        28 + i32::from(leap(year)),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day > month_days[month - 1] {
        return None;
    }
    let preceding_years = |y: i32| {
        let before = y - 1;
        before * 365 + before / 4 - before / 100 + before / 400
    };
    Some(
        preceding_years(year) - preceding_years(1970)
            + month_days[..month - 1].iter().sum::<i32>()
            + day
            - 1,
    )
}
