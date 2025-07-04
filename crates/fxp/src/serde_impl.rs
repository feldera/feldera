use serde::{de::Unexpected, ser::SerializeStruct, Serialize, Serializer};
use smallstr::SmallString;

use crate::Fixed;
use std::{
    fmt::{self, Write},
    str::FromStr,
};

const FIXED_KEY_TOKEN: &str = "$serde_json::private::Number";

struct FixedVisitor<const P: usize, const S: usize>;

impl<const P: usize, const S: usize> Serialize for Fixed<P, S> {
    /// Serializes this `Fixed` in the same form used by [serde_json::Number]
    /// when the `arbitrary_precision` feature is turned on.
    ///
    /// [serde_json::Number]: https://docs.rs/serde_json/latest/serde_json/struct.Number.html
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        let mut string = SmallString::<[u8; 64]>::new();
        write!(&mut string, "{}", self).unwrap();

        let mut s = serializer.serialize_struct(FIXED_KEY_TOKEN, 1)?;
        s.serialize_field(FIXED_KEY_TOKEN, string.as_str())?;
        s.end()
    }
}

impl<'de, const P: usize, const S: usize> serde::Deserialize<'de> for Fixed<P, S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(FixedVisitor)
    }
}

impl<'de, const P: usize, const S: usize> serde::de::Visitor<'de> for FixedVisitor<P, S> {
    type Value = Fixed<P, S>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a fixed-point number between {} and {}, inclusive",
            Self::Value::MIN,
            Self::Value::MAX
        )
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Fixed::try_from(value).map_err(|_| E::invalid_value(Unexpected::Signed(value), &self))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Fixed::try_from(value).map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Fixed::try_from(value).map_err(|_| E::invalid_value(Unexpected::Float(value), &self))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Self::Value::from_str(value).map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut map = map;
        let value = map.next_key::<FixedKey>()?;
        if value.is_none() {
            return Err(serde::de::Error::invalid_type(Unexpected::Map, &self));
        }
        let v: FixedFromString<P, S> = map.next_value()?;
        Ok(v.value)
    }
}

struct FixedKey;

impl<'de> serde::de::Deserialize<'de> for FixedKey {
    fn deserialize<D>(deserializer: D) -> Result<FixedKey, D::Error>
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
        Ok(FixedKey)
    }
}

pub struct FixedFromString<const P: usize, const S: usize> {
    pub value: Fixed<P, S>,
}

impl<'de, const P: usize, const S: usize> serde::de::Deserialize<'de> for FixedFromString<P, S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct Visitor<const P: usize, const S: usize>;

        impl<const P: usize, const S: usize> serde::de::Visitor<'_> for Visitor<P, S> {
            type Value = FixedFromString<P, S>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string containing a FIXED")
            }

            fn visit_str<E>(self, value: &str) -> Result<FixedFromString<P, S>, E>
            where
                E: serde::de::Error,
            {
                let d = Fixed::from_str(value)
                    .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(value), &self))?;
                Ok(FixedFromString { value: d })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use crate::Fixed;

    #[test]
    fn serialize() {
        type F = Fixed<10, 2>;

        assert_eq!(
            serde_json::to_string(&F::try_from(1.23).unwrap()).unwrap(),
            json!("1.23")
        );
    }

    #[test]
    fn deserialize() {
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
    }
}
