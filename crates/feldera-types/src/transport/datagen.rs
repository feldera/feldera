use std::collections::HashMap;
use std::default::Default;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use utoipa::ToSchema;

/// Strategy used to generate values.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum DatagenStrategy {
    /// Whether the field should be incremented for each new
    /// record rather than generated randomly.
    ///
    /// The default increment size is:
    /// - 1 for number types.
    /// - 1 millisecond for timestamp and time types.
    /// - 1 day for the date type.
    ///
    /// The step size can be increased/decreased with the `scale` parameter.
    #[default]
    Increment,
    /// A uniform random distribution is chosen to generate the value.
    Uniform,
    /// A Zipf distribution is chosen with the specified exponent (defined in field `e`) and
    /// `n` (which is set automatically) for the range `[1..n]` to generate the value in.
    ///
    /// Note that the Zipf distribution is only available for numbers or types that
    /// specify `values` or `range`.
    ///
    /// - In case `values` is set, the `n` is set to `values.len()`.
    /// - In case `values` is not set, `n` is set to the length of the `range`.
    /// - In case `range` is not set, the `n` is set to cover the default range of the type.
    Zipf,
    // Next are various methods to generate random strings, they are only applicable for string
    // types.
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
    #[serde(rename = "ipv4")]
    IPv4,
    #[serde(rename = "ipv6")]
    IPv6,
    #[serde(rename = "ip")]
    IP,
    #[serde(rename = "mac_address")]
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

/// Configuration for generating random data for a field of a table.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RngFieldSettings {
    /// Percentage of records where this field should be set to NULL.
    ///
    /// If not set, the generator will produce only records with non-NULL values.
    /// If set to `1..=100`, the generator will produce records with NULL values with the specified percentage.
    pub null_percentage: Option<usize>,

    /// Random strategy used to generate the value.
    pub strategy: DatagenStrategy,

    /// An optional, exclusive range [a, b) to limit the range of values the generator should produce.
    ///
    /// - For integer/floating point types specifies min/max values as an integer.
    ///   If not set, the generator will produce values for the entire range of the type for number types.
    /// - For string/binary types specifies min/max length as an integer, values are required to be >=0.
    ///   If not set, a range of [0, 25) is used by default.
    /// - For timestamp types specifies the min/max as two strings in the RFC 3339 format
    ///   (e.g., ["2021-01-01T00:00:00Z", "2022-01-02T00:00:00Z"]).
    ///   Alternatively, the range values can be specified as a number of non-leap
    ///   milliseconds since January 1, 1970 0:00:00.000 UTC (aka “UNIX timestamp”).
    ///   If not set, a range of ["1970-01-01T00:00:00Z", "2100-01-01T00:00:00Z") or [0, 4102444800000)
    ///   is used by default.
    /// - For time types specifies the min/max as two strings in the "HH:MM:SS" format.
    ///   Alternatively, the range values can be specified in milliseconds as two positive integers.
    ///   If not set, the range is 24h.
    /// - For date types, the min/max range is specified as two strings in the "YYYY-MM-DD" format.
    ///   Alternatively, two integers that represent number of days since January 1, 1970 can be used.
    ///   If not set, a range of ["1970-01-01", "2100-01-01") or [0, 54787) is used by default.
    /// - For array types specifies the min/max number of elements as an integer.
    ///   If not set, a range of [0, 5) is used by default. Range values are required to be >=0.
    /// - For map types specifies the min/max number of key-value pairs as an integer.
    ///   If not set, a range of [0, 5) is used by default.
    /// - For struct/boolean/null types `range` is ignored.
    #[schema(value_type=Object)]
    pub range: Option<(JsonValue, JsonValue)>,

    /// A scale factor to apply a multiplier to the generated value.
    ///
    /// - For integer/floating point types, the value is multiplied by the scale factor.
    /// - For timestamp types, the generated value (milliseconds) is multiplied by the scale factor.
    /// - For time types, the generated value (milliseconds) is multiplied by the scale factor.
    /// - For date types, the generated value (days) is multiplied by the scale factor.
    /// - For string/binary/array/map/struct/boolean/null types, the scale factor is ignored.
    ///
    /// - If `values` is specified, the scale factor is ignored.
    /// - If `range` is specified and the range is required to be positive (struct, map, array etc.)
    ///   the scale factor is required to be positive too.
    ///
    /// The default scale factor is 1.
    pub scale: i64,

    /// The frequency rank exponent for the Zipf distribution.
    ///
    /// - This value is only used if the strategy is set to `Zipf`.
    /// - The default value is 1.0.
    // TODO: make this f64 after API merge
    pub e: i64,

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

    /// Specifies the values that the generator should produce for the value in case the field is of type map,
    /// array or a binary/varbinary.
    pub value: Option<Box<RngFieldSettings>>,
}

/// Derive the default for `RngFieldSettings`.
///
/// We to implement this ourselves because this needs
/// to match the serde default semantics which sets scale to 1.
impl Default for RngFieldSettings {
    fn default() -> Self {
        Self {
            null_percentage: None,
            strategy: DatagenStrategy::default(),
            range: None,
            scale: 1,
            e: 1,
            values: None,
            fields: None,
            key: None,
            value: None,
        }
    }
}

/// A random generation plan for a table that generates either a limited amount of rows or runs continuously.
#[derive(Default, Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
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

    /// When multiple workers are used, each worker will pick a consecutive "chunk" of
    /// records to generate.
    ///
    /// By default, if not specified, the generator will use the formula `min(rate, 10_000)`
    /// to determine it. This works well in most situations. However, if you're
    /// running tests with lateness and many workers you can e.g., reduce the
    /// chunk size to make sure a smaller range of records is being ingested in parallel.
    ///
    /// This also controls the sizes of input batches.  If, for example, `rate`
    /// and `worker_chunk_size` are both 1000, with a single worker, the
    /// generator will output 1000 records once a second.  But if we reduce
    /// `worker_chunk_size` to 100 without changing `rate`, the generator will
    /// instead output 100 records 10 times per second.
    ///
    /// # Example
    /// Assume you generate a total of 125 records with 4 workers and a chunk size of 25.
    /// In this case, worker A will generate records 0..25, worker B will generate records 25..50,
    /// etc. A, B, C, and D will generate records in parallel. The first worker to finish its chunk
    /// will pick up the last chunk of records (100..125) to generate.
    pub worker_chunk_size: Option<usize>,

    /// Specifies the values that the generator should produce.
    pub fields: HashMap<String, Box<RngFieldSettings>>,
}

/// Configuration for generating random data for a table.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DatagenInputConfig {
    /// The sequence of generations to perform.
    ///
    /// If not set, the generator will produce a single sequence with default settings.
    /// If set, the generator will produce the specified sequences in sequential order.
    ///
    /// Note that if one of the sequences before the last one generates an unlimited number of rows
    /// the following sequences will not be executed.
    pub plan: Vec<GenerationPlan>,

    /// Number of workers to use for generating data.
    pub workers: usize,

    /// Optional seed for the random generator.
    ///
    /// Setting this to a fixed value will make the generator produce the same sequence of records
    /// every time the pipeline is run.
    ///
    /// # Notes
    /// - To ensure the set of generated input records is deterministic across multiple runs,
    ///   apart from setting a seed, `workers` also needs to remain unchanged.
    /// - The input will arrive in non-deterministic order if `workers > 1`.
    pub seed: Option<u64>,
}

impl Default for DatagenInputConfig {
    fn default() -> Self {
        Self {
            plan: vec![GenerationPlan::default()],
            workers: 1,
            seed: None,
        }
    }
}
