//! Operations on Decimal values

use crate::{
    cast_to_SqlDecimal_s, some_existing_operator, some_function2, some_operator,
    some_polymorphic_function1, some_polymorphic_function2, unwrap_cast, HasOne, HasZero,
    SqlString,
};
use dbsp::algebra::{MulByRef, OptionWeightType, F64};
use dec::{
    Context, Decimal, InvalidPrecisionError, ParseDecimalError, Rounding, TryFromDecimalError,
};
use feldera_types::deserialize_without_context;
use feldera_types::serde_with_context::{
    serde_config::DecimalFormat, SerializeWithContext, SqlSerdeConfig,
};
use num::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Zero};
use rkyv::{string::StringResolver, DeserializeUnsized, Fallible, SerializeUnsized};
use serde::de::Unexpected;
use serde::ser::Error as SerError;
use serde::{Serialize, Serializer};
use serde_json::Number;
use size_of::SizeOf;
use std::str::FromStr;
use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::{Add, AddAssign, Div, Mul, Neg, Sub},
    sync::LazyLock,
};

const MAX_DIGITS: usize = 13;
const DECIMAL_MAX_PRECISION: usize = 3 * MAX_DIGITS;

pub type LargeDecimal = Decimal<MAX_DIGITS>;

#[derive(Copy, Debug, Default, Clone)]
pub struct SqlDecimal {
    value: LargeDecimal,
}

pub(crate) type DecimalContext = Context<LargeDecimal>;

#[doc(hidden)]
pub(crate) fn is_error(context: &DecimalContext) -> bool {
    let status = context.status();
    status.insufficient_storage()
        || status.overflow()
        || status.invalid_operation()
        || status.division_by_zero()
        || status.division_undefined()
        || status.division_impossible()
        || status.division_by_zero()
        || status.division_undefined()
}

#[doc(hidden)]
pub fn get_standard_context() -> DecimalContext {
    let mut cx = DecimalContext::default();
    // Default rounding mode
    cx.set_rounding(Rounding::Down);
    // This is 38, conveniently matching the max precision supported by severall SQL dialects
    cx.set_max_exponent(DECIMAL_MAX_PRECISION as isize - 1)
        .unwrap();
    cx.set_min_exponent(-(DECIMAL_MAX_PRECISION as isize))
        .unwrap();
    cx
}

impl SizeOf for SqlDecimal {
    #[doc(hidden)]
    fn size_of_children(&self, _context: &mut size_of::Context) {}
}

impl SqlDecimal {
    pub fn new(value: LargeDecimal) -> Self {
        Self { value }
    }

    pub fn get_dec(&self) -> LargeDecimal {
        self.value
    }

    /// Return Some(self) if the context indicates no errors
    pub fn if_ok(self, context: &DecimalContext) -> Option<Self> {
        if is_error(context) {
            None
        } else {
            Some(self)
        }
    }

    pub fn validate(value: LargeDecimal, context: &DecimalContext, msg: &str) -> Self {
        if is_error(context) {
            panic!("Error during DECIMAL computation: {}", msg);
        }
        if value.is_zero() && value.is_negative() {
            SqlDecimal::new(LargeDecimal::zero())
        } else {
            SqlDecimal::new(value)
        }
    }

    pub fn abs(self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.abs(&mut result);
        Self::new(result)
    }

    pub fn rescale(&mut self, scale: i32) {
        let mut context = get_standard_context();
        context.rescale(&mut self.value, &LargeDecimal::from(scale));
    }

    pub fn round_to_place(&mut self, scale: usize) -> Result<(), InvalidPrecisionError> {
        let mut context = get_standard_context();
        context.round_to_place(&mut self.value, scale)
    }

    pub fn shift(&mut self, scale: i32) {
        let mut context = get_standard_context();
        context.shift(&mut self.value, &LargeDecimal::from(scale));
    }

    pub fn is_negative(self) -> bool {
        self.value.is_negative()
    }

    pub fn parse(value: &str) -> Result<SqlDecimal, ParseDecimalError> {
        let mut context = get_standard_context();
        let result = context.parse(value)?;
        Ok(Self::new(result))
    }

    pub fn from_i128_with_scale(value: i128, scale: i32) -> Self {
        let mut context = get_standard_context();
        let mut result = context.from_i128(value);
        let scale = LargeDecimal::from(-scale);
        context.rescale(&mut result, &scale);
        context.shift(&mut result, &scale);
        Self::validate(result, &context, "from_i128_with_scale")
    }

    pub fn from_f32(value: f32) -> Self {
        Self {
            value: LargeDecimal::from(value),
        }
    }

    pub fn from_f64(value: f64) -> Self {
        Self {
            value: LargeDecimal::from(value),
        }
    }

    pub fn mantissa(self) -> Result<i128, TryFromDecimalError> {
        let mut context = get_standard_context();
        let zero_scale = LargeDecimal::from(0);
        let mut result = self.value;
        let scale = LargeDecimal::from(-result.exponent());
        context.shift(&mut result, &scale);
        context.rescale(&mut result, &zero_scale);
        context.try_into_i128(result)
    }
}

impl Hash for SqlDecimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.to_raw_parts().hash::<H>(state);
    }
}

impl OptionWeightType for SqlDecimal {}
impl OptionWeightType for &SqlDecimal {}

impl serde::Serialize for SqlDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.value.to_standard_notation_string();
        let n: Number = s.parse::<Number>().map_err(|e| {
            S::Error::custom(format!("Error converting DECIMAL to JSON number {}", e))
        })?;
        n.serialize(serializer)
    }
}

struct DecimalVisitor;

impl<'de> serde::Deserialize<'de> for SqlDecimal {
    fn deserialize<D>(deserializer: D) -> Result<SqlDecimal, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(DecimalVisitor)
    }
}

impl<'de> serde::de::Visitor<'de> for DecimalVisitor {
    type Value = SqlDecimal;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "A DECIMAL type representing a fixed-point number"
        )
    }

    fn visit_i64<E>(self, value: i64) -> Result<SqlDecimal, E>
    where
        E: serde::de::Error,
    {
        Ok(SqlDecimal::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<SqlDecimal, E>
    where
        E: serde::de::Error,
    {
        Ok(SqlDecimal::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<SqlDecimal, E>
    where
        E: serde::de::Error,
    {
        Ok(SqlDecimal::from(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<SqlDecimal, E>
    where
        E: serde::de::Error,
    {
        SqlDecimal::parse(value).map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
    }

    fn visit_map<A>(self, map: A) -> Result<SqlDecimal, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut map = map;
        let value = map.next_key::<DecimalKey>()?;
        if value.is_none() {
            return Err(serde::de::Error::invalid_type(Unexpected::Map, &self));
        }
        let v: DecimalFromString = map.next_value()?;
        Ok(v.value)
    }
}

struct DecimalKey;
const DECIMAL_KEY_TOKEN: &str = "$serde_json::private::Number";

impl<'de> serde::de::Deserialize<'de> for DecimalKey {
    fn deserialize<D>(deserializer: D) -> Result<DecimalKey, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl serde::de::Visitor<'_> for FieldVisitor {
            type Value = ();

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid DECIMAL field")
            }

            fn visit_str<E>(self, s: &str) -> Result<(), E>
            where
                E: serde::de::Error,
            {
                if s == DECIMAL_KEY_TOKEN {
                    Ok(())
                } else {
                    Err(serde::de::Error::custom("expected field with custom name"))
                }
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)?;
        Ok(DecimalKey)
    }
}

pub struct DecimalFromString {
    pub value: SqlDecimal,
}

impl<'de> serde::de::Deserialize<'de> for DecimalFromString {
    fn deserialize<D>(deserializer: D) -> Result<DecimalFromString, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = DecimalFromString;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string containing a DECIMAL")
            }

            fn visit_str<E>(self, value: &str) -> Result<DecimalFromString, E>
            where
                E: serde::de::Error,
            {
                let d = SqlDecimal::parse(value).map_err(serde::de::Error::custom)?;
                Ok(DecimalFromString { value: d })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

//////////////////////

impl SerializeWithContext<SqlSerdeConfig> for SqlDecimal {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.decimal_format {
            DecimalFormat::Numeric => Serialize::serialize(&self, serializer),
            DecimalFormat::String => serializer.serialize_str(&self.value.to_string()),
        }
    }
}

deserialize_without_context!(SqlDecimal);

impl rkyv::Archive for SqlDecimal {
    type Archived = ();
    type Resolver = StringResolver;

    #[inline]
    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<S: Fallible + ?Sized> rkyv::Serialize<S> for SqlDecimal
where
    str: SerializeUnsized<S>,
{
    #[inline]
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<D: Fallible + ?Sized> rkyv::Deserialize<SqlDecimal, D> for ()
where
    str: DeserializeUnsized<str, D>,
{
    #[inline]
    fn deserialize(&self, _: &mut D) -> Result<SqlDecimal, D::Error> {
        unimplemented!();
    }
}

impl fmt::Display for SqlDecimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl FromStr for SqlDecimal {
    type Err = <LargeDecimal as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { value: s.parse()? })
    }
}

impl PartialEq for SqlDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Zero for SqlDecimal {
    fn zero() -> Self {
        Self {
            value: LargeDecimal::zero(),
        }
    }

    fn is_zero(&self) -> bool {
        self.value.is_zero()
    }
}

impl One for SqlDecimal {
    fn one() -> Self {
        let result = LargeDecimal::from(1);
        Self::new(result)
    }
}

impl From<i8> for SqlDecimal {
    fn from(n: i8) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<i16> for SqlDecimal {
    fn from(n: i16) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<i32> for SqlDecimal {
    fn from(n: i32) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<i64> for SqlDecimal {
    fn from(n: i64) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<u64> for SqlDecimal {
    fn from(n: u64) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<f32> for SqlDecimal {
    fn from(n: f32) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<f64> for SqlDecimal {
    fn from(n: f64) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<isize> for SqlDecimal {
    fn from(n: isize) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<usize> for SqlDecimal {
    fn from(n: usize) -> Self {
        Self::new(LargeDecimal::from(n))
    }
}

impl From<&str> for SqlDecimal {
    fn from(value: &str) -> Self {
        let mut context = get_standard_context();
        match context.parse(value) {
            Err(e) => panic!("Invalid DECIMAL value {}: {}", value, e),
            Ok(n) => Self::new(LargeDecimal::from(n)),
        }
    }
}

impl HasZero for SqlDecimal {
    #[inline]
    fn is_zero(&self) -> bool {
        Zero::is_zero(self)
    }

    #[inline]
    fn zero() -> Self {
        Zero::zero()
    }
}

impl HasOne for SqlDecimal {
    #[inline]
    fn one() -> Self {
        One::one()
    }
}

impl TryInto<i8> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<i8, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.clear_status();
        context.try_into_i8(val)
    }
}

impl TryInto<i16> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<i16, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.clear_status();
        context.try_into_i16(val)
    }
}

impl TryInto<i32> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<i32, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.clear_status();
        context.try_into_i32(val)
    }
}

impl TryInto<i64> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<i64, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.clear_status();
        context.try_into_i64(val)
    }
}

impl TryInto<f32> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<f32, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.try_into_f32(val)
    }
}

impl TryInto<f64> for SqlDecimal {
    type Error = TryFromDecimalError;

    fn try_into(self) -> Result<f64, Self::Error> {
        let mut context = get_standard_context();
        context.set_rounding(Rounding::Down);
        let mut val = self.value;
        context.round(&mut val);
        context.try_into_f64(val)
    }
}

impl Eq for SqlDecimal {}

impl PartialOrd for SqlDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SqlDecimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut context = get_standard_context();
        // This returns Option for NaN, Inf, and other illegal values
        context.partial_cmp(&self.value, &other.value).unwrap()
    }
}

impl Add for SqlDecimal {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.add(&mut result, &other.value);
        Self::validate(result, &context, "Addition")
    }
}

impl Add for &SqlDecimal {
    type Output = SqlDecimal;

    fn add(self, other: Self) -> SqlDecimal {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.add(&mut result, &other.value);
        SqlDecimal::validate(result, &context, "Addition")
    }
}

impl Neg for SqlDecimal {
    type Output = Self;

    fn neg(self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.neg(&mut result);
        Self::validate(result, &context, "Negation")
    }
}

impl Neg for &SqlDecimal {
    type Output = SqlDecimal;

    fn neg(self) -> SqlDecimal {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.neg(&mut result);
        SqlDecimal::validate(result, &context, "Negation")
    }
}

impl AddAssign<SqlDecimal> for SqlDecimal {
    fn add_assign(&mut self, other: Self) {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.add(&mut result, &other.value);
        if is_error(&context) {
            panic!("Error during DECIMAL computation: ADD");
        }
        self.value = result;
    }
}

impl AddAssign<&SqlDecimal> for SqlDecimal {
    fn add_assign(&mut self, other: &SqlDecimal) {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.add(&mut result, &other.value);
        if is_error(&context) {
            panic!("Error during DECIMAL computation: ADD");
        }
        self.value = result;
    }
}

impl CheckedAdd for SqlDecimal {
    fn checked_add(&self, other: &Self) -> Option<Self> {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.add(&mut result, &other.value);
        Self::new(result).if_ok(&context)
    }
}

impl Sub for SqlDecimal {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.sub(&mut result, &other.value);
        Self::validate(result, &context, "Subtraction")
    }
}

impl CheckedSub for SqlDecimal {
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.sub(&mut result, &other.value);
        Self::new(result).if_ok(&context)
    }
}

impl Mul for SqlDecimal {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &other.value);
        Self::validate(result, &context, "Multiplication")
    }
}

impl Mul for &SqlDecimal {
    type Output = SqlDecimal;

    fn mul(self, other: Self) -> SqlDecimal {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &other.value);
        SqlDecimal::validate(result, &context, "Multiplication")
    }
}

impl MulByRef<isize> for SqlDecimal {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &isize) -> Self::Output {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &LargeDecimal::from(*w));
        Self::validate(result, &context, "Multiplication")
    }
}

impl MulByRef<i64> for SqlDecimal {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i64) -> Self::Output {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &LargeDecimal::from(*w));
        Self::validate(result, &context, "Multiplication")
    }
}

impl MulByRef<i32> for SqlDecimal {
    type Output = Self;

    #[inline]
    fn mul_by_ref(&self, w: &i32) -> Self::Output {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &LargeDecimal::from(*w));
        Self::validate(result, &context, "Multiplication")
    }
}

impl CheckedMul for SqlDecimal {
    fn checked_mul(&self, other: &Self) -> Option<Self> {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.mul(&mut result, &other.value);
        Self::new(result).if_ok(&context)
    }
}

impl Div for SqlDecimal {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        let mut context = get_standard_context();
        let mut result = self.value;
        context.div(&mut result, &other.value);
        Self::validate(result, &context, "Division")
    }
}

impl CheckedDiv for SqlDecimal {
    fn checked_div(&self, other: &Self) -> Option<Self> {
        if other.value == LargeDecimal::zero() {
            panic!("attempt to divide by zero");
        }
        let mut context = get_standard_context();
        let mut result = self.value;
        context.div(&mut result, &other.value);
        Self::new(result).if_ok(&context)
    }
}

#[inline(always)]
#[doc(hidden)]
fn sqldecimal_modulo(left: SqlDecimal, right: SqlDecimal) -> SqlDecimal {
    if right.value == LargeDecimal::zero() {
        panic!("attempt to divide by zero");
    }
    let left_neg = left.is_negative();
    let mut context = get_standard_context();
    let mut result = left.value;
    context.abs(&mut result);
    let mut right = right.value;
    context.abs(&mut right);
    context.rem(&mut result, &right);
    if left_neg {
        context.neg(&mut result);
    }
    SqlDecimal::validate(result, &context, "Modulo")
}

some_operator!(
    sqldecimal_modulo,
    modulo,
    SqlDecimal,
    SqlDecimal,
    SqlDecimal
);

fn generic_round(left: SqlDecimal, right: i32, mode: Rounding, message: &str) -> SqlDecimal {
    if right >= (DECIMAL_MAX_PRECISION as i32) || right >= num::abs(left.value.exponent()) {
        return left;
    }
    let mut context = get_standard_context();
    context.set_rounding(mode);
    if right.is_negative() {
        let mut result = left.value;
        context.scaleb(&mut result, &LargeDecimal::from(right));
        context.round(&mut result);
        context.scaleb(&mut result, &LargeDecimal::from(-right));
        SqlDecimal::validate(result, &context, message)
    } else {
        // This is more robust to avoid overflow
        let mut result = left.value;
        let mut one = LargeDecimal::from(1);
        context.scaleb(&mut one, &LargeDecimal::from(-right));
        context.quantize(&mut result, &one);
        SqlDecimal::validate(result, &context, message)
    }
}

#[doc(hidden)]
pub fn bround__(left: SqlDecimal, right: i32) -> SqlDecimal {
    generic_round(left, right, Rounding::HalfEven, "BROUND")
}

some_function2!(bround, SqlDecimal, i32, SqlDecimal);

#[doc(hidden)]
#[inline(always)]
pub fn new_decimal(s: &str, precision: u32, scale: u32) -> Option<SqlDecimal> {
    Some(unwrap_cast(cast_to_SqlDecimal_s(
        SqlString::from(s),
        precision,
        scale,
    )))
}

#[doc(hidden)]
#[inline(always)]
pub fn round_SqlDecimal_i32(left: SqlDecimal, right: i32) -> SqlDecimal {
    generic_round(left, right, Rounding::HalfEven, "ROUND")
}

some_polymorphic_function2!(round, SqlDecimal, SqlDecimal, i32, i32, SqlDecimal);

#[doc(hidden)]
#[inline(always)]
pub fn truncate_SqlDecimal_i32(left: SqlDecimal, right: i32) -> SqlDecimal {
    generic_round(left, right, Rounding::Down, "TRUNCATE")
}

some_polymorphic_function2!(truncate, SqlDecimal, SqlDecimal, i32, i32, SqlDecimal);

#[doc(hidden)]
pub fn power_i32_SqlDecimal(left: i32, right: SqlDecimal) -> F64 {
    F64::new((left as f64).powf(right.try_into().unwrap()))
}

some_polymorphic_function2!(power, i32, i32, SqlDecimal, SqlDecimal, F64);

#[doc(hidden)]
pub static POINT_FIVE: LazyLock<LargeDecimal> = LazyLock::new(|| {
    let mut five = LargeDecimal::from(5);
    let mut context = get_standard_context();
    let minone = LargeDecimal::from(-1);
    context.rescale(&mut five, &minone);
    five
});

#[doc(hidden)]
pub fn power_SqlDecimal_SqlDecimal(left: SqlDecimal, right: SqlDecimal) -> F64 {
    if right.value.eq(&POINT_FIVE) {
        // special case for sqrt, has higher precision than pow
        sqrt_SqlDecimal(left)
    } else {
        let mut context = get_standard_context();
        let mut result = left.get_dec();
        context.pow(&mut result, &right.get_dec());
        if is_error(&context) {
            panic!("Error during POWER computation");
        }
        F64::new(context.try_into_f64(result).unwrap())
    }
}

some_polymorphic_function2!(power, SqlDecimal, SqlDecimal, SqlDecimal, SqlDecimal, F64);

#[doc(hidden)]
pub fn power_SqlDecimal_i32(left: SqlDecimal, right: i32) -> F64 {
    let mut context = get_standard_context();
    let mut result = left.get_dec();
    let power = SqlDecimal::from(right);
    context.pow(&mut result, &power.get_dec());
    if is_error(&context) {
        panic!("Error during POWER computation");
    }
    F64::new(context.try_into_f64(result).unwrap())
}

some_polymorphic_function2!(power, SqlDecimal, SqlDecimal, i32, i32, F64);

#[doc(hidden)]
pub fn power_SqlDecimal_d(left: SqlDecimal, right: F64) -> F64 {
    // Special case to match Java pow
    if right.into_inner().is_nan() {
        return right;
    }
    let mut context = get_standard_context();
    let left = context.try_into_f64(left.get_dec()).unwrap();
    F64::new(left.powf(right.into_inner()))
}

some_polymorphic_function2!(power, SqlDecimal, SqlDecimal, d, F64, F64);

#[doc(hidden)]
pub fn sqrt_SqlDecimal(left: SqlDecimal) -> F64 {
    if left < <SqlDecimal as HasZero>::zero() {
        return F64::new(f64::NAN);
    }

    let mut context = get_standard_context();
    let mut result = left.get_dec();
    context.sqrt(&mut result);
    if is_error(&context) {
        panic!("Error during SQRT computation");
    }
    F64::new(context.try_into_f64(result).unwrap())
}

some_polymorphic_function1!(sqrt, SqlDecimal, SqlDecimal, F64);

#[doc(hidden)]
pub fn floor_SqlDecimal(value: SqlDecimal) -> SqlDecimal {
    let mut context = get_standard_context();
    context.set_rounding(Rounding::Floor);
    let mut result = value.get_dec();
    context.round(&mut result);
    SqlDecimal::validate(result, &context, "FLOOR")
}

some_polymorphic_function1!(floor, SqlDecimal, SqlDecimal, SqlDecimal);

#[doc(hidden)]
pub fn ceil_SqlDecimal(value: SqlDecimal) -> SqlDecimal {
    let mut context = get_standard_context();
    context.set_rounding(Rounding::Ceiling);
    let mut result = value.get_dec();
    context.round(&mut result);
    SqlDecimal::validate(result, &context, "CEIL")
}

some_polymorphic_function1!(ceil, SqlDecimal, SqlDecimal, SqlDecimal);

#[doc(hidden)]
pub fn sign_SqlDecimal(value: SqlDecimal) -> SqlDecimal {
    let val = value.get_dec();
    if val < LargeDecimal::zero() {
        SqlDecimal::from(-1)
    } else if val > LargeDecimal::zero() {
        SqlDecimal::from(1)
    } else {
        <SqlDecimal as HasZero>::zero()
    }
}

some_polymorphic_function1!(sign, SqlDecimal, SqlDecimal, SqlDecimal);

#[doc(hidden)]
pub fn shift_left__(left: SqlDecimal, amount: i32) -> SqlDecimal {
    let pow = LargeDecimal::from(amount);
    let mut context = get_standard_context();
    let mut result = left.get_dec();
    context.scaleb(&mut result, &pow);
    SqlDecimal::validate(result, &context, "SHIFT LEFT")
}

#[doc(hidden)]
pub fn shift_leftN_(left: Option<SqlDecimal>, amount: i32) -> Option<SqlDecimal> {
    let left = left?;
    Some(shift_left__(left, amount))
}

#[cfg(test)]
mod test {
    use crate::SqlDecimal;
    use feldera_types::deserialize_table_record;
    use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
    use size_of::SizeOf;
    use std::sync::LazyLock;

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);

    fn deserialize_with_default_context<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_str(json),
            &DEFAULT_CONFIG,
        )
    }

    #[test]
    fn sizeof_sqldecimal() {
        let d = SqlDecimal::from_i128_with_scale(12309182309182093812i128, 10);
        let total_size = SizeOf::size_of(&d);
        assert_eq!(36, total_size.total_bytes());
        assert_eq!(0, total_size.shared_bytes());
    }

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct Struct2 {
        #[allow(non_snake_case)]
        cc_num: u64,
        #[allow(non_snake_case)]
        first: Option<String>,
        #[allow(non_snake_case)]
        dec: SqlDecimal,
    }
    deserialize_table_record!(Struct2["Table.Name", 3] {(cc_num, "cc_num", false, u64, None), (first, "first", false, Option<String>, Some(None)), (dec, "dec", false, SqlDecimal, None)});

    #[test]
    fn deserialize_struct2() {
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"cc_num": 100, "dec": "0.123"}"#)
                .unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(0.123),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"cc_num": 100, "dec": 0.123}"#)
                .unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(0.123),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": null, "dec": "-1.40"}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(-1.40),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": null, "dec": -1.40}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(-1.40),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": "foo", "dec": "1e20"}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: 100,
                first: Some("foo".to_string()),
                dec: SqlDecimal::from(1e20),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"first": "foo"}"#)
                .map_err(|e| e.to_string()),
            Err(r#"missing field `cc_num` at line 1 column 16"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, "foo", "-1e20"]"#).unwrap(),
            Struct2 {
                cc_num: 100,
                first: Some("foo".to_string()),
                dec: SqlDecimal::from(-1e20),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, "2e-5"]"#).unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(0.00002),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, "-3e-5"]"#).unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(-3e-5),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, -3e-5]"#).unwrap(),
            Struct2 {
                cc_num: 100,
                first: None,
                dec: SqlDecimal::from(-3e-5),
            }
        );
    }
}
