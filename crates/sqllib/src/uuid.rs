//! Uuid operations

use dbsp::NumEntries;
use feldera_types::serde_with_context::{
    serde_config::UuidFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::{de, de::Error as _, Deserializer, Serializer};
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

    /// Parse a string into a Uuid
    #[doc(hidden)]
    pub fn from_string(value: &String) -> Self {
        Self {
            value: uuid::Uuid::parse_str(value)
                .unwrap_or_else(|_| panic!("Cannot parse {value} into a UUID")),
        }
    }

    /// Parse a string into a Uuid
    #[doc(hidden)]
    pub fn from_ref(value: &str) -> Self {
        Self {
            value: uuid::Uuid::parse_str(value)
                .unwrap_or_else(|_| panic!("Cannot parse {value} into a UUID")),
        }
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.value, f)
    }
}
