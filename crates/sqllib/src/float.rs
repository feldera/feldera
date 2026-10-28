use crate::{
    SqlString, finite_or_null, some_function1, some_polymorphic_function1,
    some_polymorphic_function2, some_polymorphic_null_function1,
};
use dbsp::algebra::{F32, F64};
use lexical_core::format::STANDARD;
use lexical_core::{ToLexicalWithOptions, WriteFloatOptions};
use num_traits::{Float, NumCast, Zero};
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

/// Saturating form of `ln_d`: negative infinity, the smallest
/// DOUBLE, for a negative argument, -0.0 included.
#[doc(hidden)]
pub fn ln_d_saturating(left: F64) -> F64 {
    if left.into_inner().is_sign_negative() {
        F64::new(f64::NEG_INFINITY)
    } else {
        ln_d(left)
    }
}

/// Saturating form of `log10_d`, like [`ln_d_saturating`].
#[doc(hidden)]
pub fn log10_d_saturating(left: F64) -> F64 {
    if left.into_inner().is_sign_negative() {
        F64::new(f64::NEG_INFINITY)
    } else {
        log10_d(left)
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn log_d(left: F64) -> F64 {
    ln_d(left)
}

some_polymorphic_function1!(log, d, F64, F64);

/// Saturating form of `log_d`, like [`ln_d_saturating`].
#[doc(hidden)]
pub fn log_d_saturating(left: F64) -> F64 {
    ln_d_saturating(left)
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

/// Saturating form of `log_d_d` in its first argument: negative infinity, the
/// smallest DOUBLE, for a negative first argument, -0.0 included.
#[doc(hidden)]
pub fn log_d_d_saturating(left: F64, right: F64) -> F64 {
    if left.into_inner().is_sign_negative() {
        F64::new(f64::NEG_INFINITY)
    } else {
        log_d_d(left, right)
    }
}

/// Returns `round(value * 10^digits) / 10^digits`, without -0.  When `value * 10^digits` is not
/// finite, `value` has no digits at the position `digits`, so the result is `value`.
fn round_to_digits<T: Float>(value: T, digits: i32, round: fn(T) -> T) -> T {
    let tens = <T as NumCast>::from(10).unwrap().powi(digits);
    let scaled = value * tens;
    let result = if scaled.is_finite() {
        let rounded = round(scaled);
        // When `10^digits` is 0, `rounded` is 0 too.
        if rounded.is_zero() {
            rounded
        } else {
            rounded / tens
        }
    } else {
        value
    };
    if result.is_zero() { T::zero() } else { result }
}

#[doc(hidden)]
#[inline(always)]
pub fn truncate_d_i32(left: F64, right: i32) -> F64 {
    round_to_digits(left.into_inner(), right, f64::trunc).into()
}

some_polymorphic_function2!(truncate, d, F64, i32, i32, F64);

#[doc(hidden)]
#[inline(always)]
pub fn truncate_f_i32(left: F32, right: i32) -> F32 {
    round_to_digits(left.into_inner(), right, f32::trunc).into()
}

some_polymorphic_function2!(truncate, f, F32, i32, i32, F32);

// The compiler names TRUNC and TRUNCATE by their SQL spelling.
#[doc(hidden)]
#[inline(always)]
pub fn trunc_d_i32(left: F64, right: i32) -> F64 {
    truncate_d_i32(left, right)
}

some_polymorphic_function2!(trunc, d, F64, i32, i32, F64);

#[doc(hidden)]
#[inline(always)]
pub fn trunc_f_i32(left: F32, right: i32) -> F32 {
    truncate_f_i32(left, right)
}

some_polymorphic_function2!(trunc, f, F32, i32, i32, F32);

#[doc(hidden)]
#[inline(always)]
pub fn round_d_i32(left: F64, right: i32) -> F64 {
    round_to_digits(left.into_inner(), right, f64::round).into()
}

some_polymorphic_function2!(round, d, F64, i32, i32, F64);

#[doc(hidden)]
#[inline(always)]
pub fn round_f_i32(left: F32, right: i32) -> F32 {
    round_to_digits(left.into_inner(), right, f32::round).into()
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
pub fn sqrt_d(left: F64) -> F64 {
    let left = left.into_inner();
    F64::new(left.sqrt())
}

some_polymorphic_function1!(sqrt, d, F64, F64);

/// Saturating form of `sqrt_d`: negative infinity, the smallest
/// DOUBLE, for a negative argument, whose square root is NaN, the
/// largest DOUBLE.
#[doc(hidden)]
pub fn sqrt_d_saturating(left: F64) -> F64 {
    if left.into_inner() < 0.0 {
        F64::new(f64::NEG_INFINITY)
    } else {
        sqrt_d(left)
    }
}

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

#[inline(always)]
#[doc(hidden)]
pub fn finite_or_null_d(value: F64) -> Option<F64> {
    finite_or_null(value.into_inner()).map(|x| x.into())
}

some_polymorphic_null_function1!(finite_or_null, d, F64, F64);

#[doc(hidden)]
pub fn finite_or_null_f(value: F32) -> Option<F32> {
    finite_or_null(value.into_inner()).map(|x| x.into())
}

some_polymorphic_null_function1!(finite_or_null, f, F32, F32);

#[cfg(test)]
mod test {
    use super::{
        ln_d_saturating, log_d_d_saturating, log_d_saturating, log10_d_saturating, round_d_i32,
        round_f_i32, sqrt_d_saturating, trunc_d_i32, trunc_f_i32, truncate_d_i32,
    };
    use dbsp::algebra::{F32, F64};

    #[test]
    fn round_to_many_digits() {
        // 10^digits is infinity or 0, or the scaled value is not finite.
        assert_eq!(round_d_i32(F64::new(1.5), 400), F64::new(1.5));
        assert_eq!(truncate_d_i32(F64::new(1.5), 400), F64::new(1.5));
        assert_eq!(round_d_i32(F64::new(1.5), -400), F64::new(0.0));
        assert_eq!(round_d_i32(F64::new(1e300), 20), F64::new(1e300));
        assert_eq!(round_f_i32(F32::new(1.5), 50), F32::new(1.5));
        assert_eq!(round_d_i32(F64::new(1.25), 1), F64::new(1.3));
    }

    #[test]
    fn trunc_is_truncate() {
        assert_eq!(trunc_d_i32(F64::new(1.29), 1), F64::new(1.2));
        assert_eq!(trunc_f_i32(F32::new(-1.29), 1), F32::new(-1.2));
    }

    #[test]
    fn saturating_logarithms() {
        let minus_infinity = F64::new(f64::NEG_INFINITY);
        assert_eq!(ln_d_saturating(F64::new(-9.0)), minus_infinity);
        assert_eq!(ln_d_saturating(F64::new(-0.0)), minus_infinity);
        assert_eq!(ln_d_saturating(F64::new(0.0)), minus_infinity);
        assert_eq!(ln_d_saturating(F64::new(1.0)), F64::new(0.0));
        assert_eq!(log10_d_saturating(F64::new(-1.0)), minus_infinity);
        assert_eq!(log10_d_saturating(F64::new(100.0)), F64::new(2.0));
        assert_eq!(log_d_saturating(F64::new(-1.0)), minus_infinity);
        let two = F64::new(2.0);
        assert_eq!(log_d_d_saturating(F64::new(-9.0), two), minus_infinity);
        assert_eq!(log_d_d_saturating(two, two), F64::new(1.0));
        assert_eq!(sqrt_d_saturating(F64::new(-9.0)), minus_infinity);
        assert_eq!(sqrt_d_saturating(F64::new(-0.0)), F64::new(-0.0));
        assert_eq!(sqrt_d_saturating(F64::new(9.0)), F64::new(3.0));
    }
}
