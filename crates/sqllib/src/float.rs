use crate::{
    some_function1, some_polymorphic_function1, some_polymorphic_function2, SqlDecimal, SqlString,
};
use dbsp::algebra::{F32, F64};
use lexical_core::format::STANDARD;
use lexical_core::{ToLexicalWithOptions, WriteFloatOptions};
use num_traits::{Pow, Zero};
use std::sync::LazyLock;

#[doc(hidden)]
#[inline(always)]
pub fn exp_d(value: F64) -> F64 {
    value.into_inner().exp().into()
}

some_polymorphic_function1!(exp, d, F64, F64);

#[doc(hidden)]
#[inline(always)]
pub fn ln_d(left: F64) -> F64 {
    let left = left.into_inner();

    if left.is_sign_negative() {
        panic!("Unable to calculate ln for {left}");
    }

    left.ln().into()
}

some_polymorphic_function1!(ln, d, F64, F64);

#[doc(hidden)]
#[inline(always)]
pub fn log10_d(left: F64) -> F64 {
    let left = left.into_inner();

    if left.is_sign_negative() {
        panic!("Unable to calculate log10 for {left}");
    }

    left.log10().into()
}

some_polymorphic_function1!(log10, d, F64, F64);

#[doc(hidden)]
#[inline(always)]
pub fn log_d(left: F64) -> F64 {
    ln_d(left)
}

some_polymorphic_function1!(log, d, F64, F64);

fn normalize_zero_d(v: f64) -> f64 {
    if v.is_zero() {
        // avoid -0
        0.0f64
    } else {
        v
    }
}

fn normalize_zero_f(v: f32) -> f32 {
    if v.is_zero() {
        // avoid -0
        0.0f32
    } else {
        v
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn log_d_d(left: F64, right: F64) -> F64 {
    let left = left.into_inner();
    let right = right.into_inner();

    if left.is_sign_negative() || right.is_sign_negative() {
        panic!("Unable to calculate log({left}, {right})")
    }

    // match Calcite's behavior, return 0 instead of -0
    if right.is_zero() {
        return F64::new(0.0);
    }

    left.log(right).into()
}

some_polymorphic_function2!(log, d, F64, d, F64, F64);

#[doc(hidden)]
#[inline(always)]
pub fn truncate_d_i32(left: F64, right: i32) -> F64 {
    let tens = 10.0_f64.pow(right);
    let mut left = left.into_inner() * tens;
    left = left.trunc();
    left /= tens;
    left = normalize_zero_d(left);
    (left).into()
}

some_polymorphic_function2!(truncate, d, F64, i32, i32, F64);

#[doc(hidden)]
#[inline(always)]
pub fn truncate_f_i32(left: F32, right: i32) -> F32 {
    let tens = 10.0_f32.pow(right);
    let mut left = left.into_inner() * tens;
    left = left.trunc();
    left /= tens;
    left = normalize_zero_f(left);
    (left).into()
}

some_polymorphic_function2!(truncate, f, F32, i32, i32, F32);

#[doc(hidden)]
#[inline(always)]
pub fn round_d_i32(left: F64, right: i32) -> F64 {
    let tens = 10.0_f64.pow(right);
    let mut left = left.into_inner() * tens;
    left = left.round();
    left /= tens;
    left = normalize_zero_d(left);
    (left).into()
}

some_polymorphic_function2!(round, d, F64, i32, i32, F64);

#[doc(hidden)]
#[inline(always)]
pub fn round_f_i32(left: F32, right: i32) -> F32 {
    let tens = 10.0_f32.pow(right);
    let mut left = left.into_inner() * tens;
    left = left.round();
    left /= tens;
    left = normalize_zero_f(left);
    (left).into()
}

some_polymorphic_function2!(round, f, F32, i32, i32, F32);

#[doc(hidden)]
pub fn power_i32_d(left: i32, right: F64) -> F64 {
    F64::new((left as f64).powf(right.into_inner()))
}

some_polymorphic_function2!(power, i32, i32, d, F64, F64);

#[doc(hidden)]
pub fn power_d_i32(left: F64, right: i32) -> F64 {
    F64::new(left.into_inner().powi(right))
}

some_polymorphic_function2!(power, d, F64, i32, i32, F64);

#[doc(hidden)]
pub fn power_d_d(left: F64, right: F64) -> F64 {
    if right.into_inner().is_nan() {
        return right;
    }
    F64::new(left.into_inner().powf(right.into_inner()))
}

some_polymorphic_function2!(power, d, F64, d, F64, F64);

#[doc(hidden)]
pub fn power_d_SqlDecimal(left: F64, right: SqlDecimal) -> F64 {
    F64::new(left.into_inner().powf(right.try_into().unwrap()))
}

some_polymorphic_function2!(power, d, F64, SqlDecimal, SqlDecimal, F64);

#[doc(hidden)]
pub fn sqrt_d(left: F64) -> F64 {
    let left = left.into_inner();
    F64::new(left.sqrt())
}

some_polymorphic_function1!(sqrt, d, F64, F64);

//////////////////// floor /////////////////////

#[inline(always)]
#[doc(hidden)]
pub fn floor_d(value: F64) -> F64 {
    F64::new(value.into_inner().floor())
}

#[inline(always)]
#[doc(hidden)]
pub fn floor_f(value: F32) -> F32 {
    F32::new(value.into_inner().floor())
}

some_polymorphic_function1!(floor, f, F32, F32);
some_polymorphic_function1!(floor, d, F64, F64);

//////////////////// ceil /////////////////////

#[inline(always)]
#[doc(hidden)]
pub fn ceil_d(value: F64) -> F64 {
    F64::new(value.into_inner().ceil())
}

#[inline(always)]
#[doc(hidden)]
pub fn ceil_f(value: F32) -> F32 {
    F32::new(value.into_inner().ceil())
}

some_polymorphic_function1!(ceil, f, F32, F32);
some_polymorphic_function1!(ceil, d, F64, F64);

///////////////////// sign //////////////////////

#[inline(always)]
#[doc(hidden)]
pub fn sign_d(value: F64) -> F64 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f64 {
        value
    } else {
        F64::new(x.signum())
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_f(value: F32) -> F32 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f32 {
        value
    } else {
        F32::new(x.signum())
    }
}

some_polymorphic_function1!(sign, f, F32, F32);
some_polymorphic_function1!(sign, d, F64, F64);

// PI
#[inline(always)]
#[doc(hidden)]
pub fn pi() -> F64 {
    std::f64::consts::PI.into()
}

/////////// Trigonometric Functions //////////////

#[inline(always)]
#[doc(hidden)]
pub fn sin_d(value: F64) -> F64 {
    value.into_inner().sin().into()
}

some_polymorphic_function1!(sin, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn cos_d(value: F64) -> F64 {
    value.into_inner().cos().into()
}

some_polymorphic_function1!(cos, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn tan_d(value: F64) -> F64 {
    value.into_inner().tan().into()
}

some_polymorphic_function1!(tan, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn sec_d(value: F64) -> F64 {
    (1.0 / value.into_inner().cos()).into()
}

some_polymorphic_function1!(sec, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn csc_d(value: F64) -> F64 {
    (1.0 / value.into_inner().sin()).into()
}

some_polymorphic_function1!(csc, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn cot_d(value: F64) -> F64 {
    (1.0_f64 / value.into_inner().tan()).into()
}

some_polymorphic_function1!(cot, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn asin_d(value: F64) -> F64 {
    value.into_inner().asin().into()
}

some_polymorphic_function1!(asin, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn acos_d(value: F64) -> F64 {
    value.into_inner().acos().into()
}

some_polymorphic_function1!(acos, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn atan_d(value: F64) -> F64 {
    value.into_inner().atan().into()
}

some_polymorphic_function1!(atan, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn atan2_d_d(y: F64, x: F64) -> F64 {
    let y = y.into_inner();
    let x = x.into_inner();

    y.atan2(x).into()
}

some_polymorphic_function2!(atan2, d, F64, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn degrees_d(value: F64) -> F64 {
    value.into_inner().to_degrees().into()
}

some_polymorphic_function1!(degrees, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn radians_d(value: F64) -> F64 {
    value.into_inner().to_radians().into()
}

some_polymorphic_function1!(radians, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn cbrt_d(value: F64) -> F64 {
    value.into_inner().cbrt().into()
}

some_polymorphic_function1!(cbrt, d, F64, F64);

/////////// Hyperbolic Functions //////////////

#[inline(always)]
#[doc(hidden)]
pub fn sinh_d(value: F64) -> F64 {
    value.into_inner().sinh().into()
}

some_polymorphic_function1!(sinh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn cosh_d(value: F64) -> F64 {
    value.into_inner().cosh().into()
}

some_polymorphic_function1!(cosh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn tanh_d(value: F64) -> F64 {
    value.into_inner().tanh().into()
}

some_polymorphic_function1!(tanh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn coth_d(value: F64) -> F64 {
    (1.0 / value.into_inner().tanh()).into()
}

some_polymorphic_function1!(coth, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn asinh_d(value: F64) -> F64 {
    value.into_inner().asinh().into()
}

some_polymorphic_function1!(asinh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn acosh_d(value: F64) -> F64 {
    if value.into_inner() < 1.0 {
        panic!("input ({}) out of range [1, Infinity]", value)
    }

    value.into_inner().acosh().into()
}

some_polymorphic_function1!(acosh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn atanh_d(value: F64) -> F64 {
    let inner = value.into_inner();
    if !(-1.0..=1.0).contains(&inner) && !inner.is_nan() {
        panic!("input ({}) out of range [-1, 1]", value)
    }

    inner.atanh().into()
}

some_polymorphic_function1!(atanh, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn csch_d(value: F64) -> F64 {
    (1.0 / value.into_inner().sinh()).into()
}

some_polymorphic_function1!(csch, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn sech_d(value: F64) -> F64 {
    (1.0 / value.into_inner().cosh()).into()
}

some_polymorphic_function1!(sech, d, F64, F64);

#[inline(always)]
#[doc(hidden)]
pub fn is_inf_d(value: F64) -> bool {
    value.into_inner().is_infinite()
}

#[inline(always)]
#[doc(hidden)]
pub fn is_inf_f(value: F32) -> bool {
    value.into_inner().is_infinite()
}

some_polymorphic_function1!(is_inf, d, F64, bool);
some_polymorphic_function1!(is_inf, f, F32, bool);

#[inline(always)]
#[doc(hidden)]
pub fn is_nan_d(value: F64) -> bool {
    value.into_inner().is_nan()
}

#[inline(always)]
#[doc(hidden)]
pub fn is_nan_f(value: F32) -> bool {
    value.into_inner().is_nan()
}

some_polymorphic_function1!(is_nan, d, F64, bool);
some_polymorphic_function1!(is_nan, f, F32, bool);

/// Convert a REAL value to a string, with 6 digits of precision
/// This is used to make tests involving FP values more robust; instead
/// of comparing FP values, we only compare such strings.
pub fn to_string_f_(value: F32) -> SqlString {
    static F32_OPTIONS: LazyLock<WriteFloatOptions> = LazyLock::new(|| {
        lexical_core::WriteFloatOptions::builder()
            // Only write up to 6 significant digits, IE, `1.234567` becomes `1.23456`.
            .max_significant_digits(std::num::NonZeroUsize::new(6))
            .min_significant_digits(std::num::NonZeroUsize::new(0))
            // Trim the trailing `.0` from integral float strings.
            .trim_floats(true)
            .build()
            .unwrap()
    });

    let mut buffer = [0u8; 128]; // Allocate buffer
    let bytes = value
        .into_inner()
        .to_lexical_with_options::<{ STANDARD }>(&mut buffer, &F32_OPTIONS);
    String::from_utf8_lossy(bytes).into_owned().into()
}

some_function1!(to_string_f, F32, SqlString);

/// Convert a DOUBLE value to a string, with 15 digits of precision.
/// This is used to make tests involving FP values more robust; instead
/// of comparing FP values, we only compare such strings.
pub fn to_string_d_(value: F64) -> SqlString {
    static F64_OPTIONS: LazyLock<WriteFloatOptions> = LazyLock::new(|| {
        lexical_core::WriteFloatOptions::builder()
            .max_significant_digits(std::num::NonZeroUsize::new(14))
            .min_significant_digits(std::num::NonZeroUsize::new(0))
            .trim_floats(true)
            .build()
            .unwrap()
    });

    let mut buffer = [0u8; 128];
    let bytes = value
        .into_inner()
        .to_lexical_with_options::<{ STANDARD }>(&mut buffer, &F64_OPTIONS);
    String::from_utf8_lossy(bytes).into_owned().into()
}

some_function1!(to_string_d, F64, SqlString);

#[test]
pub fn check() {
    assert_eq!("1.2", to_string_f_(1.2f32.into()).str());
    assert_eq!("1.2", to_string_d_(1.2f64.into()).str());
    assert_eq!("1.23e-10", to_string_f_(0.000000000123f32.into()).str());
    assert_eq!("1.23e-10", to_string_d_(0.000000000123f64.into()).str());
    assert_eq!("1.23e10", to_string_f_(12300000000f32.into()).str());
    assert_eq!("1.23e10", to_string_d_(12300000000f64.into()).str());
    assert_eq!(
        Some(SqlString::from("1.23e10")),
        to_string_fN(Some(12300000000f32.into()))
    );
    assert_eq!(
        Some(SqlString::from("1.23e10")),
        to_string_dN(Some(12300000000f64.into()))
    );
}
