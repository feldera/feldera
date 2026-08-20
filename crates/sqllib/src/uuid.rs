//! Uuid operations

use crate::error::{SqlResult, SqlRuntimeError};
use dbsp::NumEntries;
use feldera_macros::IsNone;
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig, serde_config::UuidFormat,
};
use serde::{Deserializer, Serializer, de, de::Error as _};
use size_of::{Context, SizeOf};
use std::fmt::{self, Debug, Display};

/// A type for storing universally unique identifiers.
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Uuid {
    value: uuid::Uuid,
}

impl From<uuid::Uuid> for Uuid {
    fn from(uuid: uuid::Uuid) -> Self {
        Uuid { value: uuid }
    }
}

impl SizeOf for Uuid {
    fn size_of_children(&self, _context: &mut Context) {}
}

impl SerializeWithContext<SqlSerdeConfig> for Uuid {
    fn serialize_with_context<S>(
        &self,
        serializer: S,
        context: &SqlSerdeConfig,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match context.uuid_format {
            UuidFormat::String => serializer.serialize_str(&self.value.to_string()),
            UuidFormat::Binary => serializer.serialize_bytes(self.value.as_bytes()),
        }
    }
}

#[doc(hidden)]
impl NumEntries for &Uuid {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[doc(hidden)]
    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[doc(hidden)]
    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

impl<'de, AUX> DeserializeWithContext<'de, SqlSerdeConfig, AUX> for Uuid {
    fn deserialize_with_context<D>(
        deserializer: D,
        config: &'de SqlSerdeConfig,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // We use config.uuid_format as a hint to deserializer, but do our best effort to deserialize
        // from any supported representation: string, bytes, or byte array.

        fn de_error<E: de::Error>(e: uuid::Error) -> E {
            E::custom(format_args!("UUID parsing failed: {}", e))
        }

        struct UuidVisitor;

        impl<'vi> de::Visitor<'vi> for UuidVisitor {
            type Value = Uuid;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a UUID string or byte array")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Uuid, E> {
                Ok(Uuid {
                    value: value.parse::<uuid::Uuid>().map_err(de_error)?,
                })
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Uuid, E> {
                Ok(Uuid {
                    value: uuid::Uuid::from_slice(value).map_err(de_error)?,
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Uuid, A::Error>
            where
                A: de::SeqAccess<'vi>,
            {
                #[rustfmt::skip]
                let bytes = [
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(0, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(1, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(2, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(3, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(4, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(5, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(6, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(7, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(8, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(9, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(10, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(11, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(12, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(13, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(14, &self)) },
                    match seq.next_element()? { Some(e) => e, None => return Err(A::Error::invalid_length(15, &self)) },
                ];

                Ok(Uuid::from_bytes(bytes))
            }
        }

        match config.uuid_format {
            UuidFormat::String => deserializer.deserialize_str(UuidVisitor),
            UuidFormat::Binary => deserializer.deserialize_bytes(UuidVisitor),
        }
    }
}

impl Uuid {
    /// Allocate Uuid from a byte array of correct endianness
    pub fn from_bytes(data: [u8; 16]) -> Self {
        Self {
            value: uuid::Uuid::from_bytes(data),
        }
    }

    /// Emit the Uuid as a byte slice
    pub fn to_bytes(&self) -> &[u8; 16] {
        self.value.as_bytes()
    }

    /// Parse a string into a Uuid.
    ///
    /// A UUID is written as 32 hexadecimal digits, optionally wrapped in braces.
    /// Hyphens may separate groups of four digits, so all of the following denote
    /// the same value:
    ///
    /// ```text
    /// 123e4567-e89b-12d3-a456-426655440000
    /// 123E4567-E89B-12D3-A456-426655440000
    /// 123e4567e89b12d3a456426655440000
    /// {123e4567-e89b-12d3-a456-426655440000}
    /// 123e-4567-e89b-12d3-a456-4266-5544-0000
    /// ```
    ///
    /// Blanks are never trimmed, and the `urn:uuid:` prefix is not accepted.
    #[doc(hidden)]
    pub fn try_from_ref(value: &str) -> SqlResult<Self> {
        match Self::parse(value) {
            Some(value) => Ok(Self { value }),
            // Quoted, so that leading and trailing blanks are visible
            None => Err(SqlRuntimeError::from_string(format!(
                "Invalid UUID string '{value}'"
            ))),
        }
    }

    /// Grammar shared with the SQL compiler, which parses UUID literals in Java.
    fn parse(value: &str) -> Option<uuid::Uuid> {
        let bytes = value.as_bytes();
        let body = match (bytes.first(), bytes.last()) {
            (Some(b'{'), Some(b'}')) if bytes.len() > 1 => &bytes[1..bytes.len() - 1],
            _ => bytes,
        };

        let mut digits = [0u8; 32];
        let mut count = 0;
        let mut previous = 0u8;
        for &c in body {
            if c == b'-' {
                // A hyphen separates groups, so it must follow a complete group
                // of four digits and cannot be the last character
                if count == 0 || count % 4 != 0 || count == 32 || previous == b'-' {
                    return None;
                }
            } else if c.is_ascii_hexdigit() && count < 32 {
                digits[count] = c;
                count += 1;
            } else {
                return None;
            }
            previous = c;
        }
        if count != 32 {
            return None;
        }

        let digits = std::str::from_utf8(&digits).ok()?;
        Some(uuid::Uuid::from_u128(
            u128::from_str_radix(digits, 16).ok()?,
        ))
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.value, f)
    }
}

#[doc(hidden)]
pub fn uuid_to_u128_(u: Uuid) -> u128 {
    u128::from_be_bytes(*u.to_bytes())
}

#[doc(hidden)]
pub fn u128_to_uuid_(n: u128) -> Uuid {
    Uuid::from_bytes(n.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::Uuid;

    const EXPECTED: u128 = 0x123e4567_e89b_12d3_a456_426655440000;

    fn parsed(value: &str) -> Uuid {
        Uuid::try_from_ref(value).unwrap_or_else(|e| panic!("{value:?}: {e}"))
    }

    #[test]
    fn accepted_spellings() {
        // Every spelling accepted by the SQL compiler denotes the same value
        for value in [
            "123e4567-e89b-12d3-a456-426655440000",
            "123E4567-E89B-12D3-A456-426655440000",
            "123e4567e89b12d3a456426655440000",
            "{123e4567-e89b-12d3-a456-426655440000}",
            "{123e4567e89b12d3a456426655440000}",
            "123e-4567-e89b-12d3-a456-4266-5544-0000",
            "123e4567-e89b12d3-a4564266-55440000",
            "123e-4567e89b-12d3a456426655440000",
        ] {
            assert_eq!(super::uuid_to_u128_(parsed(value)), EXPECTED, "{value:?}");
        }
    }

    #[test]
    fn rejected_spellings() {
        for value in [
            "",
            "   ",
            "1-2-3-4-5",                             // a group is not four digits wide
            "123e456-7e89b-12d3-a456-426655440000",  // as above, though 36 characters long
            "123e4567--e89b-12d3-a456-426655440000", // empty group
            "-123e4567e89b12d3a456426655440000",     // leading hyphen
            "123e4567e89b12d3a456426655440000-",     // trailing hyphen
            "{123e4567-e89b-12d3-a456-426655440000", // unbalanced brace
            "123e4567-e89b-12d3-a456-42665544000",   // 31 digits
            "123e4567-e89b-12d3-a456-4266554400000", // 33 digits
            " 123e4567-e89b-12d3-a456-426655440000", // blanks are not trimmed
            "123e4567-e89b-12d3-a456-426655440000 ",
            "urn:uuid:123e4567-e89b-12d3-a456-426655440000", // URN form is not accepted
            "123e4567-e89b-12d3-a456-42665544000g",          // not a hexadecimal digit
            "{}",
        ] {
            let error = Uuid::try_from_ref(value)
                .err()
                .unwrap_or_else(|| panic!("{value:?} should be rejected"));
            assert_eq!(error.to_string(), format!("Invalid UUID string '{value}'"));
        }
    }
}
