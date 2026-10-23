use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use feldera_fxp::DynamicDecimal;
use feldera_types::variant::{VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantValue};
use serde::Deserialize;
use serde::de::{
    self, DeserializeSeed, Deserializer, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};

/// Connector metadata attached to each input record.
///
/// Both the transport connector and the parser can add metadata attributes
/// such as Kafka topic name or Avro schema id. These attributes are passed
/// to the deserializer along with the actual record, which can use them
/// to populate some of the table columns.
///
/// An attribute is a [`VariantValue`], which any runtime can read. A clone
/// shares the attributes until one of the two changes, so a record
/// deserializer can tell by [`Self::ptr_eq`] that it already converted them.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct ConnectorMetadata(Arc<BTreeMap<String, VariantValue>>);

impl ConnectorMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the attribute `name`, replacing any attribute already stored
    /// under that name.
    pub fn insert(&mut self, name: &str, value: VariantValue) {
        Arc::make_mut(&mut self.0).insert(name.to_string(), value);
    }

    /// Returns the attribute stored under `name`, or `None` if absent.
    pub fn get_by_name(&self, name: &str) -> Option<&VariantValue> {
        self.0.get(name)
    }

    /// Returns `true` if no attribute has been added.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the attributes in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &VariantValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    /// Returns the attributes by name.
    pub fn attributes(&self) -> &BTreeMap<String, VariantValue> {
        &self.0
    }

    /// Returns `true` if `self` and `other` are the same value, not only equal
    /// ones: one is a clone of the other, and neither changed since.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Returns one attribute for each field of the JSON object `json`.
    ///
    /// Each field converts the way JSON converts to a VARIANT. An integer that
    /// fits `i64` becomes an `Int64`, and a larger one that fits `u64` a
    /// `Decimal16` of scale 0. Any other number becomes the narrowest decimal
    /// that holds it. Of two fields with one name, the last one counts.
    ///
    /// # Arguments
    ///
    /// * `json` - The text of a JSON object.
    ///
    /// # Returns
    ///
    /// The attributes, or why `json` is refused: it is not JSON, it holds a
    /// value that a [`VariantValue`] cannot represent, or it is not an object.
    pub fn from_json_object(json: &str) -> Result<Self, JsonMetadataError> {
        let value: JsonVariant = serde_json::from_str(json).map_err(JsonMetadataError::Json)?;
        match value
            .into_variant()
            .map_err(JsonMetadataError::Unrepresentable)?
        {
            VariantValue::Object(attributes) => Ok(Self::from(attributes)),
            _ => Err(JsonMetadataError::NotAnObject),
        }
    }
}

/// Why [`ConnectorMetadata::from_json_object`] refused its input.
#[derive(Debug)]
pub enum JsonMetadataError {
    /// The text is not JSON, or it holds a number that no decimal can hold.
    Json(serde_json::Error),
    /// A value that a [`VariantValue`] cannot represent, such as a number
    /// with more digits than the widest VARIANT decimal holds.
    Unrepresentable(String),
    /// A JSON value that is not an object.
    NotAnObject,
}

impl fmt::Display for JsonMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "not valid JSON: {error}"),
            Self::Unrepresentable(error) => {
                write!(
                    f,
                    "holds a value that connector metadata cannot represent: {error}"
                )
            }
            Self::NotAnObject => f.write_str("not a JSON object"),
        }
    }
}

impl std::error::Error for JsonMetadataError {}

/// A JSON value as the VARIANT JSON parser reads it, before it becomes a
/// [`VariantValue`].
///
/// With serde_json's `arbitrary_precision` feature, which the workspace turns
/// on, an integer that fits `u64` or `i64` arrives as that integer, and every
/// other number arrives as its text, which [`DynamicDecimal`] parses.
enum JsonVariant {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Double(f64),
    Decimal(DynamicDecimal),
    String(String),
    List(Vec<JsonVariant>),
    /// The fields in the order of the text, with any repeated names.
    Object(Vec<(String, JsonVariant)>),
}

impl JsonVariant {
    /// Returns `self` as a [`VariantValue`].
    ///
    /// An object keeps the last of the fields that share a name, and converts
    /// its fields in name order, so the first value that cannot convert is the
    /// one with the smallest name.
    fn into_variant(self) -> Result<VariantValue, String> {
        Ok(match self {
            Self::Null => VariantValue::Null,
            Self::Boolean(value) => VariantValue::Boolean(value),
            Self::Signed(value) => VariantValue::Int64(value),
            Self::Unsigned(value) => unsigned_value(value),
            Self::Double(value) => VariantValue::Double(value),
            Self::Decimal(value) => decimal_value(value.significand(), value.exponent())?,
            Self::String(value) => VariantValue::String(value),
            Self::List(items) => VariantValue::List(
                items
                    .into_iter()
                    .map(Self::into_variant)
                    .collect::<Result<_, _>>()?,
            ),
            Self::Object(fields) => {
                let fields: BTreeMap<String, JsonVariant> = fields.into_iter().collect();
                VariantValue::Object(
                    fields
                        .into_iter()
                        .map(|(name, value)| Ok((name, value.into_variant()?)))
                        .collect::<Result<_, String>>()?,
                )
            }
        })
    }
}

/// Returns `value` as the smallest signed type that holds it: an `Int64` when
/// it fits, and a decimal of scale 0 when it does not.
fn unsigned_value(value: u64) -> VariantValue {
    match i64::try_from(value) {
        Ok(value) => VariantValue::Int64(value),
        Err(_) => VariantValue::Decimal16(
            VariantDecimal16::try_new(value.into(), 0).expect("a u64 has at most 20 digits"),
        ),
    }
}

/// Returns `integer * 10^-scale` in the narrowest decimal width that holds it.
/// A width can take `integer`'s type and still refuse its digits or its scale,
/// so a refusal moves on to the next width.
fn decimal_value(integer: i128, scale: u8) -> Result<VariantValue, String> {
    if let Ok(narrow) = i32::try_from(integer)
        && let Ok(decimal) = VariantDecimal4::try_new(narrow, scale)
    {
        return Ok(VariantValue::Decimal4(decimal));
    }
    if let Ok(narrow) = i64::try_from(integer)
        && let Ok(decimal) = VariantDecimal8::try_new(narrow, scale)
    {
        return Ok(VariantValue::Decimal8(decimal));
    }
    VariantDecimal16::try_new(integer, scale)
        .map(VariantValue::Decimal16)
        .map_err(|error| format!("cannot convert a VARIANT decimal to a VariantValue: {error}"))
}

impl<'de> Deserialize<'de> for JsonVariant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonVariantVisitor)
    }
}

struct JsonVariantVisitor;

impl<'de> Visitor<'de> for JsonVariantVisitor {
    type Value = JsonVariant;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonVariant::Boolean(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonVariant::Signed(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonVariant::Unsigned(value))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
        Ok(JsonVariant::Decimal(DynamicDecimal::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(JsonVariant::Double(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(JsonVariant::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(JsonVariant::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonVariant::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonVariant::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(JsonVariant::List(items))
    }

    /// serde_json delivers an arbitrary-precision number as a map with one
    /// entry, under a private key. A JSON object that uses the key as a field
    /// name is still an object; [`NumberToken`] tells the two apart.
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let first = match map.next_key_seed(KeyClassifier)? {
            None => return Ok(JsonVariant::Object(Vec::new())),
            Some(KeyClass::Field(name)) => (name, map.next_value()?),
            Some(KeyClass::Number) => match map.next_value_seed(NumberToken)? {
                TokenValue::Number(text) => {
                    let number = DynamicDecimal::deserialize(
                        IntoDeserializer::<A::Error>::into_deserializer(text),
                    )?;
                    return Ok(JsonVariant::Decimal(number));
                }
                TokenValue::Entry(value) => (NUMBER_TOKEN.to_string(), value),
            },
        };
        let mut fields = vec![first];
        while let Some(name) = map.next_key::<String>()? {
            fields.push((name, map.next_value()?));
        }
        Ok(JsonVariant::Object(fields))
    }
}

/// serde_json's private key for an arbitrary-precision number.
const NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// Tells serde_json's private number key from a field name.
struct KeyClassifier;

enum KeyClass {
    Field(String),
    Number,
}

impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for KeyClassifier {
    type Value = KeyClass;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E>(self, key: &str) -> Result<Self::Value, E> {
        Ok(match key {
            NUMBER_TOKEN => KeyClass::Number,
            _ => KeyClass::Field(key.to_owned()),
        })
    }

    fn visit_string<E>(self, key: String) -> Result<Self::Value, E> {
        Ok(match key.as_str() {
            NUMBER_TOKEN => KeyClass::Number,
            _ => KeyClass::Field(key),
        })
    }
}

/// Reads the value under the private number key. serde_json delivers the text
/// of a number with `visit_string`, and the strings of JSON text with
/// `visit_str` or `visit_borrowed_str`; any other value belongs to an object
/// that uses the key as a field name.
struct NumberToken;

enum TokenValue {
    /// The text of a JSON number.
    Number(String),
    /// The value of an object field that the key names.
    Entry(JsonVariant),
}

impl<'de> DeserializeSeed<'de> for NumberToken {
    type Value = TokenValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for NumberToken {
    type Value = TokenValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        JsonVariantVisitor.expecting(formatter)
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<Self::Value, E> {
        Ok(TokenValue::Number(text))
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_bool(value).map(TokenValue::Entry)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_i64(value).map(TokenValue::Entry)
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_u64(value).map(TokenValue::Entry)
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_i128(value).map(TokenValue::Entry)
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_f64(value).map(TokenValue::Entry)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_str(value).map(TokenValue::Entry)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_none().map(TokenValue::Entry)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        JsonVariantVisitor.visit_unit().map(TokenValue::Entry)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        JsonVariantVisitor
            .visit_some(deserializer)
            .map(TokenValue::Entry)
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        JsonVariantVisitor.visit_seq(seq).map(TokenValue::Entry)
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        JsonVariantVisitor.visit_map(map).map(TokenValue::Entry)
    }
}

impl From<BTreeMap<String, VariantValue>> for ConnectorMetadata {
    fn from(metadata: BTreeMap<String, VariantValue>) -> Self {
        Self(Arc::new(metadata))
    }
}

impl From<ConnectorMetadata> for VariantValue {
    fn from(metadata: ConnectorMetadata) -> Self {
        VariantValue::Object(Arc::unwrap_or_clone(metadata.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An attribute reads back under the name it was added with.
    #[test]
    fn test_get_by_name_returns_inserted_attribute() {
        let mut metadata = ConnectorMetadata::new();
        assert!(metadata.is_empty());
        metadata.insert("topic", VariantValue::String("events".into()));

        assert_eq!(
            metadata.get_by_name("topic"),
            Some(&VariantValue::String("events".into()))
        );
        assert_eq!(metadata.get_by_name("missing"), None);
        assert!(!metadata.is_empty());
        assert_eq!(
            VariantValue::from(metadata),
            VariantValue::Object(BTreeMap::from([(
                "topic".to_string(),
                VariantValue::String("events".into())
            )]))
        );
    }

    /// A clone is the same value until one of the two changes, and the change
    /// leaves the other as it was.
    #[test]
    fn test_a_clone_is_the_same_value_until_it_changes() {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("kafka_offset", VariantValue::Int64(17));
        let mut clone = metadata.clone();
        assert!(clone.ptr_eq(&metadata));

        clone.insert("kafka_partition", VariantValue::Int32(3));
        assert!(!clone.ptr_eq(&metadata));
        assert_eq!(metadata.get_by_name("kafka_partition"), None);
        assert_eq!(
            clone.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["kafka_offset", "kafka_partition"]
        );

        // Equal attributes built apart are equal, but not the same value.
        let mut other = ConnectorMetadata::new();
        other.insert("kafka_offset", VariantValue::Int64(17));
        assert_eq!(other, metadata);
        assert!(!other.ptr_eq(&metadata));
    }

    /// Returns the attributes that `json` decodes to.
    fn attributes(json: &str) -> BTreeMap<String, VariantValue> {
        match VariantValue::from(ConnectorMetadata::from_json_object(json).unwrap()) {
            VariantValue::Object(attributes) => attributes,
            other => panic!("not an object: {other:?}"),
        }
    }

    /// Each JSON value converts the way JSON converts to a VARIANT.
    #[test]
    fn test_json_values_convert_as_json_converts_to_a_variant() {
        let attributes = attributes(
            r#"{
                "null": null, "yes": true, "text": "events",
                "small": 42, "negative": -5,
                "i64_max": 9223372036854775807, "past_i64_max": 9223372036854775808,
                "u64_max": 18446744073709551615, "below_i64_min": -9223372036854775809,
                "fraction": 1.50, "exponent": 1e3, "zero": 0.0, "wide": 12345678901.25,
                "list": [1, "a", null], "object": {"b": {"c": []}}
            }"#,
        );
        let decimal16 =
            |integer: i128| VariantValue::Decimal16(VariantDecimal16::try_new(integer, 0).unwrap());
        let expected = BTreeMap::from([
            ("null", VariantValue::Null),
            ("yes", VariantValue::Boolean(true)),
            ("text", VariantValue::String("events".into())),
            ("small", VariantValue::Int64(42)),
            ("negative", VariantValue::Int64(-5)),
            ("i64_max", VariantValue::Int64(i64::MAX)),
            ("past_i64_max", decimal16(9_223_372_036_854_775_808)),
            ("u64_max", decimal16(u64::MAX.into())),
            ("below_i64_min", decimal16(-9_223_372_036_854_775_809)),
            (
                "fraction",
                VariantValue::Decimal4(VariantDecimal4::try_new(15, 1).unwrap()),
            ),
            (
                "exponent",
                VariantValue::Decimal4(VariantDecimal4::try_new(1000, 0).unwrap()),
            ),
            (
                "zero",
                VariantValue::Decimal4(VariantDecimal4::try_new(0, 0).unwrap()),
            ),
            (
                "wide",
                VariantValue::Decimal8(VariantDecimal8::try_new(1_234_567_890_125, 2).unwrap()),
            ),
            (
                "list",
                VariantValue::List(vec![
                    VariantValue::Int64(1),
                    VariantValue::String("a".into()),
                    VariantValue::Null,
                ]),
            ),
            (
                "object",
                VariantValue::Object(BTreeMap::from([(
                    "b".to_string(),
                    VariantValue::Object(BTreeMap::from([(
                        "c".to_string(),
                        VariantValue::List(Vec::new()),
                    )])),
                )])),
            ),
        ])
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect::<BTreeMap<_, _>>();
        assert_eq!(attributes, expected);
    }

    /// Of two fields with one name, the last one counts, so a value that the
    /// last field replaces is never converted.
    #[test]
    fn test_the_last_of_two_fields_with_one_name_counts() {
        assert_eq!(
            attributes(r#"{"a": 123456789012345678901234567890123456789, "b": 1, "a": "two"}"#),
            BTreeMap::from([
                ("a".to_string(), VariantValue::String("two".into())),
                ("b".to_string(), VariantValue::Int64(1)),
            ])
        );
    }

    /// JSON text may use serde_json's private number key as a field name.
    #[test]
    fn test_the_private_number_key_is_a_field_name_in_json_text() {
        assert_eq!(
            attributes(r#"{"$serde_json::private::Number": "12"}"#),
            BTreeMap::from([(NUMBER_TOKEN.to_string(), VariantValue::String("12".into()))])
        );
        assert_eq!(
            attributes(r#"{"$serde_json::private::Number": 1.5, "b": true}"#),
            BTreeMap::from([
                (
                    NUMBER_TOKEN.to_string(),
                    VariantValue::Decimal4(VariantDecimal4::try_new(15, 1).unwrap())
                ),
                ("b".to_string(), VariantValue::Boolean(true)),
            ])
        );
    }

    /// Text that is not JSON, a value that no VARIANT holds, and JSON that is
    /// not an object are each refused with their own error.
    #[test]
    fn test_refuses_what_is_not_an_object_of_representable_values() {
        for json in ["not json", r#"{"a": 1} x"#, r#"{"a": 01}"#] {
            let error = ConnectorMetadata::from_json_object(json).unwrap_err();
            assert!(
                matches!(error, JsonMetadataError::Json(_)),
                "{json}: {error}"
            );
        }

        // A number that no decimal holds is refused while the JSON is read.
        let error = ConnectorMetadata::from_json_object(r#"{"x": 1e40}"#).unwrap_err();
        assert!(matches!(error, JsonMetadataError::Json(_)), "{error}");
        assert!(
            error
                .to_string()
                .starts_with("not valid JSON: invalid value: string \"1e40\", expected a decimal number at line 1 column "),
            "{error}"
        );

        // A number with more digits than the widest VARIANT decimal holds.
        for json in [
            r#"{"x": 123456789012345678901234567890123456789}"#,
            "[123456789012345678901234567890123456789]",
        ] {
            let error = ConnectorMetadata::from_json_object(json).unwrap_err();
            assert_eq!(
                error.to_string(),
                "holds a value that connector metadata cannot represent: cannot convert a \
                 VARIANT decimal to a VariantValue: 123456789012345678901234567890123456789 \
                 has more digits than the maximum precision 38",
                "{json}"
            );
        }

        for json in ["[1, 2]", "\"text\"", "7", "null"] {
            let error = ConnectorMetadata::from_json_object(json).unwrap_err();
            assert!(
                matches!(error, JsonMetadataError::NotAnObject),
                "{json}: {error}"
            );
        }
    }

    /// serde_json's recursion limit bounds how deeply a value nests.
    #[test]
    fn test_nesting_past_the_json_recursion_limit_is_refused() {
        let nested =
            |depth: usize| format!(r#"{{"a": {}{}}}"#, "[".repeat(depth), "]".repeat(depth));
        assert!(ConnectorMetadata::from_json_object(&nested(126)).is_ok());
        let error = ConnectorMetadata::from_json_object(&nested(127)).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("not valid JSON: recursion limit exceeded"),
            "{error}"
        );
    }
}
