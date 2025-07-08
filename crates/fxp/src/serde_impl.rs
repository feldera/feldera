use serde::{
    de::{Error, Unexpected},
    ser::SerializeStruct,
    Serialize, Serializer,
};
use smallstr::SmallString;

use crate::{DynamicDecimal, Fixed};
use std::{
    fmt::{self, Display, Write},
    str::FromStr,
};

const FIXED_KEY_TOKEN: &str = "$serde_json::private::Number";

fn serialize_helper<T, S>(value: T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Display,
    S: Serializer,
{
    let mut string = SmallString::<[u8; 64]>::new();
    write!(&mut string, "{}", value).unwrap();

    let mut s = serializer.serialize_struct(FIXED_KEY_TOKEN, 1)?;
    s.serialize_field(FIXED_KEY_TOKEN, string.as_str())?;
    s.end()
}

impl<const P: usize, const S: usize> Serialize for Fixed<P, S> {
    /// Serializes this `Fixed` in the same form used by [serde_json::Number]
    /// when the `arbitrary_precision` feature is turned on.
    ///
    /// [serde_json::Number]: https://docs.rs/serde_json/latest/serde_json/struct.Number.html
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        serialize_helper(self, serializer)
    }
}

impl Serialize for DynamicDecimal {
    /// Serializes this `DynamicDecimal` in the same form used by
    /// [serde_json::Number] when the `arbitrary_precision` feature is turned
    /// on.
    ///
    /// [serde_json::Number]: https://docs.rs/serde_json/latest/serde_json/struct.Number.html
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        serialize_helper(self, serializer)
    }
}

impl<'de, const P: usize, const S: usize> serde::Deserialize<'de> for Fixed<P, S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let decimal = DynamicDecimal::deserialize(deserializer)?;
        decimal.try_into().map_err(|_| {
            D::Error::custom(format!(
                "decimal number {decimal} is outside the value range {} to {} (inclusive)",
                Fixed::<P, S>::MIN,
                Fixed::<P, S>::MAX
            ))
        })
    }
}

impl<'de> serde::Deserialize<'de> for DynamicDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(DecimalVisitor)
    }
}

struct DecimalVisitor;

impl<'de> serde::de::Visitor<'de> for DecimalVisitor {
    type Value = DynamicDecimal;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a decimal number")
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DynamicDecimal::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DynamicDecimal::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DynamicDecimal::try_from(value)
            .map_err(|_| E::invalid_value(Unexpected::Float(value), &self))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DynamicDecimal::from_str(value).map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
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

impl<'de> serde::de::Deserialize<'de> for DecimalKey {
    fn deserialize<D>(deserializer: D) -> Result<DecimalKey, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl serde::de::Visitor<'_> for FieldVisitor {
            type Value = ();

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid FIXED field")
            }

            fn visit_str<E>(self, s: &str) -> Result<(), E>
            where
                E: serde::de::Error,
            {
                if s == FIXED_KEY_TOKEN {
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
    pub value: DynamicDecimal,
}

impl<'de> serde::de::Deserialize<'de> for DecimalFromString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = DecimalFromString;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string containing a decimal number")
            }

            fn visit_str<E>(self, value: &str) -> Result<DecimalFromString, E>
            where
                E: serde::de::Error,
            {
                let d = DynamicDecimal::from_str(value)
                    .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(value), &self))?;
                Ok(DecimalFromString { value: d })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use serde_json::json;

    use crate::{DynamicDecimal, Fixed};

    #[test]
    fn serialize_fixed() {
        type F = Fixed<10, 2>;

        assert_eq!(
            serde_json::to_string(&F::try_from(1.23).unwrap()).unwrap(),
            json!("1.23")
        );
    }

    #[test]
    fn serialize_dynamic() {
        assert_eq!(
            serde_json::to_string(&DynamicDecimal::from_str("1.23").unwrap()).unwrap(),
            json!("1.23")
        );
    }

    #[test]
    fn deserialize_fixed() {
        type F = Fixed<10, 2>;

        assert_eq!(
            serde_json::from_str::<F>("1.23").unwrap(),
            F::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<F>("\"1.23\"").unwrap(),
            F::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<F>(r#"{"$serde_json::private::Number": "1.23"}"#).unwrap(),
            F::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<F>("123").unwrap(),
            F::try_from(123).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<F>("-123").unwrap(),
            F::try_from(-123).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<F>("99999999").unwrap(),
            F::try_from(99999999).unwrap()
        );
        assert!(serde_json::from_str::<F>("999999999").is_err());
    }

    #[test]
    fn deserialize_dynamic() {
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("1.23").unwrap(),
            DynamicDecimal::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("\"1.23\"").unwrap(),
            DynamicDecimal::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>(r#"{"$serde_json::private::Number": "1.23"}"#)
                .unwrap(),
            DynamicDecimal::try_from(1.23).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("123").unwrap(),
            DynamicDecimal::try_from(123).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("-123").unwrap(),
            DynamicDecimal::try_from(-123).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("99999999").unwrap(),
            DynamicDecimal::try_from(99999999).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<DynamicDecimal>("1e38").unwrap(),
            DynamicDecimal::try_from(1e38).unwrap()
        );
        assert!(serde_json::from_str::<DynamicDecimal>("1e39").is_err());
    }
}
