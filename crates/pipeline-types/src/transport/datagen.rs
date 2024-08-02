use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use utoipa::ToSchema;

fn default_scale() -> i64 {
    1
}

fn default_exponent() -> usize {
    1
}

fn default_workers() -> usize {
    1
}

fn default_sequence() -> Vec<GenerationPlan> {
    vec![GenerationPlan::default()]
}

/// Various methods to generate different random strings.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StringMethod {
    #[default]
    Word,
    Words,
    Sentence,
    Sentences,
    Paragraph,
    Paragraphs,
    FirstName,
    LastName,
    Title,
    Suffix,
    Name,
    NameWithTitle,
    DomainSuffix,
    Email,
    Username,
    Password,
    Field,
    Position,
    Seniority,
    JobTitle,
    IPv4,
    IPv6,
    IP,
    MACAddress,
    UserAgent,
    RfcStatusCode,
    ValidStatusCode,
    CompanySuffix,
    CompanyName,
    Buzzword,
    BuzzwordMiddle,
    BuzzwordTail,
    CatchPhrase,
    BsVerb,
    BsAdj,
    BsNoun,
    Bs,
    Profession,
    Industry,
    CurrencyCode,
    CurrencyName,
    CurrencySymbol,
    CreditCardNumber,
    CityPrefix,
    CitySuffix,
    CityName,
    CountryName,
    CountryCode,
    StreetSuffix,
    StreetName,
    TimeZone,
    StateName,
    StateAbbr,
    SecondaryAddressType,
    SecondaryAddress,
    ZipCode,
    PostCode,
    BuildingNumber,
    Latitude,
    Longitude,
    Isbn,
    Isbn13,
    Isbn10,
    PhoneNumber,
    CellNumber,
    FilePath,
    FileName,
    FileExtension,
    DirPath,
}

/// Strategy used to generate values.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(tag = "name")]
pub enum DatagenStrategy {
    /// Whether the field should be incremented for each new
    /// record rather than generated randomly.
    ///
    /// A scale factor can be set as the `param` field to apply a multiplier to the increment.
    /// The default scale factor is 1.
    #[serde(rename = "increment")]
    Increment {
        #[serde(default = "default_scale")]
        scale: i64,
    },
    /// A uniform random distribution is chosen to generate the value.
    #[serde(rename = "uniform")]
    Uniform,
    /// A Zipf distribution is chosen with the specified exponent (`s`) and
    /// `n` (set automatically) for the range `[1..n]` to generate the value in.
    ///
    /// Note that the Zipf distribution is only available for numbers or types that
    /// specify `values` or `range`.
    ///
    /// - In case `values` is set, the `n` is set to `values.len()`.
    /// - In case `values` is not set, `n` is set to the length of the `range`.
    /// - In case `range` is not set, the `n` is set to cover the default range of the type.
    #[serde(rename = "zipf")]
    Zipf {
        #[serde(default = "default_exponent")]
        s: usize,
    },
    /// A strategy to produce a random string for various data.
    ///
    /// - This strategy is only available for string types.
    #[serde(rename = "string")]
    String {
        #[serde(default)]
        method: StringMethod,
    },
}

impl Default for DatagenStrategy {
    /// If `mode` is not specified, default to `Watch`.
    fn default() -> Self {
        Self::Increment { scale: 1 }
    }
}

/// Configuration for generating random data for a field of a table.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RngFieldSettings {
    /// Percentage of records where this field should be set to NULL.
    ///
    /// If not set, the generator will produce only records with non-NULL values.
    /// If set to `1..=100`, the generator will produce records with NULL values with the specified percentage.
    pub null_percentage: Option<usize>,

    /// Random strategy used to generate the value.
    #[serde(default)]
    pub strategy: DatagenStrategy,

    /// An optional, exclusive range [a, b) to limit the range of values the generator should produce.
    ///
    /// - For integer/floating point types specifies min/max values.
    ///   If not set, the generator will produce values for the entire range of the type for number types.
    /// - For string/binary types specifies min/max length, values are required to be >=0.
    ///   If not set, a range of [0, 25) is used by default.
    /// - For timestamp types specifies the min/max in milliseconds from the number of non-leap
    ///   milliseconds since January 1, 1970 0:00:00.000 UTC (aka “UNIX timestamp”).
    ///   If not set, a range of [0, 4102444800) is used by default (1970-01-01 -- 2100-01-01).
    /// - For time types specifies the min/max in milliseconds.
    ///   If not set, the range is 24h. Range values are required to be >=0.
    /// - For date types specifies the min/max in days from the number of days since January 1, 1970.
    ///   If not set, a range of [0, 54787) is used by default (1970-01-01 -- 2100-01-01).
    /// - For array types specifies the min/max number of elements.
    ///   If not set, a range of [0, 5) is used by default. Range values are required to be >=0.
    /// - For map types specifies the min/max number of key-value pairs.
    ///   If not set, a range of [0, 5) is used by default.
    /// - For struct/boolean/null types `range` is ignored.
    pub range: Option<(i64, i64)>,

    /// An optional set of values the generator will pick from.
    ///
    /// If set, the generator will pick values from the specified set.
    /// If not set, the generator will produce values according to the specified range.
    /// If set to an empty set, the generator will produce NULL values.
    /// If set to a single value, the generator will produce only that value.
    ///
    /// Note that `range` is ignored if `values` is set.
    #[schema(value_type = Option<Vec<Object>>)]
    pub values: Option<Vec<JsonValue>>,

    /// Specifies the values that the generator should produce in case the field is a struct type.
    pub fields: Option<HashMap<String, Box<RngFieldSettings>>>,

    /// Specifies the values that the generator should produce for the key in case the field is a map.
    pub key: Option<Box<RngFieldSettings>>,

    /// Specifies the values that the generator should produce for the value in case the field is a map
    /// or array.
    pub value: Option<Box<RngFieldSettings>>,
}

/// A random generation plan for a table that generates either a limited amount of rows or runs continuously.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct GenerationPlan {
    /// Non-zero number of rows to generate per second.
    ///
    /// If not set, the generator will produce rows as fast as possible.
    pub rate: Option<u32>,

    /// Total number of new rows to generate.
    ///
    /// If not set, the generator will produce new/unique records as long as the pipeline is running.
    /// If set to 0, the table will always remain empty.
    /// If set, the generator will produce new records until the specified limit is reached.
    ///
    /// Note that if the table has one or more primary keys that don't use the `increment` strategy to
    /// generate the key there is a potential that an update is generated instead of an insert. In
    /// this case it's possible the total number of records is less than the specified limit.
    pub limit: Option<usize>,

    /// Specifies the values that the generator should produce.
    #[serde(default)]
    pub fields: HashMap<String, Box<RngFieldSettings>>,
}

/// Configuration for generating random data for a table.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DatagenInputConfig {
    /// The sequence of generations to perform.
    ///
    /// If not set, the generator will produce a single sequence with default settings.
    /// If set, the generator will produce the specified sequences in sequential order.
    ///
    /// Note that if one of the sequences before the last one generates an unlimited number of rows
    /// the following sequences will not be executed.
    #[serde(default = "default_sequence")]
    pub plan: Vec<GenerationPlan>,

    /// Number of workers to use for generating data.
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Optional seed for the random generator.
    ///
    /// Setting this to a fixed value will make the generator produce the same sequence of records
    /// every time the pipeline is run.
    pub seed: Option<u64>,
}
