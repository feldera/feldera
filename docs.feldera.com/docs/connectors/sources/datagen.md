# Datagen input connector

Datagen is a source connector that generates synthetic data for testing,
prototyping and benchmarking purposes.

For a tutorial on how to use the Datagen connector, see the
[Random Data Generation](/tutorials/basics/part4) tutorial.

The datagen input connector supports [fault
tolerance](/pipelines/fault-tolerance).

## Datagen input connector configuration

### Config Parameters

All parameters are optional:

* `workers` - How many workers tasks should be spawned to generate the data. This can be used to
  increase the data generation rate. Default is 1. Note that the `rate` parameter is global and
  workers will share the rate limit.

* `seed` - Optional seed for the random number generator. When this option is set, the data
  generation process will be deterministic and produce the same output for the same seed.
  However, note that with `workers > 1` it does not guarantee that the data be inserted
  in the pipeline in the same order every time.
  By default, a random seed is used.

* `plan` - A list of plans to generate rows. See the [Plan](#plan) section for details.
  By default, a single plan is used that generates rows with incrementing values for
  every type.

### Plan

A plan is a list of objects that describe how to generate rows. Each object can have the following, optional
fields:

- `limit`: How many rows to generate. If not specified, the plan will run indefinitely.
- `rate`: How many rows to generate per second. If not specified, the plan will run as fast as possible. See also
  the `workers` parameter.
- `fields`: A map of field names to [field generation options](#random-field-settings).
- `worker_chunk_size`: When multiple workers are used, each worker will complete a consecutive "chunk" of records
  before getting a new chunk (by synchronizing with other workers). This parameter specifies the size of the chunk.
  If not specified, the field will be set to `min(rate, 10_000)`. This works well in most situations.
  However, if you're running tests with lateness and many workers you can e.g., reduce the chunk size
  to make sure a smaller range of records is being ingested/generated in parallel.

  Example: Assume datagen generates a total of 125 records with 4 workers and a chunk size of 25. In this case,
  worker A will generate records `0..25`, worker B will generate records `25..50`, etc. A, B, C, and D will
  generate the first 100 records in parallel. The first worker to finish its chunk will pick up the last chunk
  of records (`100..125`) and generate the records for it.

### Random Field Settings

Each field can set a strategy that defines how a value is picked:

* `strategy` - The strategy to use for generating values. The following strategies are available:
    * `increment` - (default) Generate an incrementing sequence of values for the given type where each value is
      greater than the previous one (wrapping around once reaching the limit of numeric types or the limits specified
      by `range`). The step size is determined by the type but can be adjusted with the `scale` parameter:
        * For integer and string types, the increment is 1.
        * For floating point types, the increment is 1.0.
        * For time and timestamp types, the increment is 1 millisecond.
        * For date types, the increment is 1 day.
        * When generating an array/binary/varbinary type, the increment will
          start from zero with a step size of 1.
    * `uniform` - Generate random values from a uniform distribution.
    * `zipf` - Generate random values from a Zipf distribution. The exponent of the distribution can be set with the
      `e` parameter.
    * For string types, the [String Generation](#string-generation-strategies) section lists more strategies
      on how to generate more specific strings.

A field can have an optional `range` parameter that defines the range of values a strategy will pick from.
The range is inclusive and contains all values with `start <= x < end`. It will return an error if `start >= end`.
The application of range depends on the type:

- For integer/floating point types specifies min/max values.
  If not set, the generator will produce values for the entire range of the type for number types.
- For string types specifies min/max length, values are required to be `>=0`.
  If not set, a range of `[0, 25)` is used by default.
- For timestamp types specifies the min/max as two strings in the RFC 3339 format
  (e.g., `["2021-01-01T00:00:00Z", "2022-01-02T00:00:00Z"]`).
  Alternatively, the range values can be specified as a number of non-leap
  milliseconds since January 1, 1970 0:00:00.000 UTC (aka “UNIX timestamp”).
  If not set, a range of `["1970-01-01T00:00:00Z", "2100-01-01T00:00:00Z")` or `[0, 4102444800000)`
  is used by default.
- For time types specifies the min/max as two strings in the `HH:MM:SS` format.
  Alternatively, the range values can be specified in milliseconds as two positive integers.
  If not set, the range is 24h.
- For date types, the min/max range is specified as two strings in the "YYYY-MM-DD" format.
  Alternatively, two integers that represent number of days since January 1, 1970 can be used.
  If not set, a range of `["1970-01-01", "2100-01-01")` or `[0, 54787)` is used by default.
- For array, binary or varbinary types specifies the min/max number of elements it should contain.
  If not set, a range of `[0, 5)` is used by default. Range values are required to be >=0.
- For map types specifies the min/max number of key-value pairs it should contain.
  If not set, a range of `[0, 5)` is used by default.
- For struct/boolean/null types `range` is ignored.

The `values` parameter can be used to specify a list of values to pick from. If set, the `range` parameter is ignored.
The values given in the list must be of the same type as the field.

A field can have an optional `scale` parameter that is applied as a multiplier to the value generated
by the strategy. The default scale is `1`. The `scale` factor is only applicable in combination with either the
`increment` or `uniform` strategy. The following rules apply:

- For integer/floating point types, the value is multiplied by the scale factor.
- For timestamp types, the generated value (milliseconds) is multiplied by the scale factor.
- For time types, the generated value (milliseconds) is multiplied by the scale factor.
- For date types, the generated value (days) is multiplied by the scale factor.
- For string/binary types, the scale factor is ignored except with the `increment` strategy where
  it applies the scale to the number that is formatted as a string.
- For array/map/struct/binary/varbinary/boolean/null types, the scale factor is ignored.
- If `values` is specified, the scale factor is ignored.

`null_percentage` adds a chance for generating `null` values for this field. If not specified, the field will
never be `null`.

#### Nested Types

If the type of the field is of type `array`, `binary` or `varbinary` the `value` parameter defines the field
settings for the elements of the array.

If the type of the field is map, the `key`/`value` define the field settings for the two types of the map.

If the type of the field is struct, the `fields` list defines the field settings for each member of the struct.

#### String Generation Strategies

In case the field type is a string, various strategies can be used to generate different kinds of strings.

- Lorem: `word`, `words`, `sentence`, `sentences`, `paragraph`, `paragraphs`
- Name: `first_name`, `last_name`, `title`, `suffix`, `name`, `name_with_title`, `phone_number`, `cell_number`
- Internet: `domain_suffix`, `email`, `username`, `password`, `ipv4`, `ipv6`, `ip`, `mac_address`, `user_agent`

Company: `company_suffix`, `company_name`, `buzzword`, `buzzword_middle`, `buzzword_tail`, `catch_phrase`, `bs_verb`, `bs_adj`, `bs_noun`, `bs`, `profession`, `industry`

- Currency: `currency_code`, `currency_name`, `currency_symbol`
- Finance: `credit_card_number`

Address: `city_prefix`, `city_suffix`, `city_name`, `country_name`, `country_code`, `street_suffix`, `street_name`, `time_zone`, `state_name`, `state_abbr`, `secondary_address_type`, `secondary_address`, `zip_code`, `post_code`, `building_number`, `latitude`, `longitude`

- Barcode: `isbn10`, `isbn13`, `isbn`
- Files: `file_path`, `file_name`, `file_extension`, `dir_path`

For some of these parameters (`words`, `sentences`, `paragraphs`) the length of the resulting string is controlled with
the `range` parameter.

## Examples

* A table with no configuration generates incrementing values for all types:

```sql
CREATE TABLE Stocks (
    symbol VARCHAR NOT NULL,
    price_time BIGINT NOT NULL,  -- UNIX timestamp
    price DOUBLE NOT NULL
) with (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {}
    }
  }]'
);
```

Will generate the following data:

```
+--------+------------+------------------+
| symbol | price_time | price            |
+--------+------------+------------------+
| 0   | 0             | 0                |
| 1   | 1             | 1                |
| 2   | 2             | 2                |
| 3   | 3             | 3                |
| 4   | 4             | 4                |
<skipped>
```

* A table with a single plan that generates 5 rows with a rate of 1 row per second:

```sql
CREATE TABLE Stocks (
    symbol VARCHAR NOT NULL,
    price_time BIGINT NOT NULL,  -- UNIX timestamp
    price DOUBLE NOT NULL
) with (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 5,
            "rate": 1,
            "fields": {
                "symbol": { "values": ["AAPL", "GOOGL", "SPY", "NVDA"] },
                "price": { "strategy": "uniform", "range": [100, 10000] }
            }
        }]
      }
    }
  }]'
);
```

Will generate the following data:

```text
+--------+------------+------------------+
| symbol | price_time | price            |
+--------+------------+------------------+
| AAPL   | 0          | 7872.823776513556|
| GOOGL  | 1          | 4942.908519064813|
| SPY    | 2          | 6120.359304755155|
| NVDA   | 3          | 2985.127163635988|
| AAPL   | 4          | 6762.121127526935|
+--------+------------+------------------+
```

* A table with a `VARBINARY` type, datagen is configured so the array will contain a growing number of elements
  from 0 and 4, each element will be a random byte in range `128..256`:

```sql
CREATE TABLE binary_tbl (
    bin VARBINARY NOT NULL
) with (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 5,
            "fields": {
                "bin": { "range": [0, 5], "value": { "strategy": "uniform", "range": [128, 256] } }
            }
        }]
      }
    }
  }]'
);
```

Will generate the following data:

```text
+----------------------+
| bin                  |
+----------------------+
| []                   |
| [128]                |
| [203, 175]           |
| [174, 228, 209]      |
| [219, 208, 161, 147] |
| []                   |
+----------------------+
```

* A table with date, timestamp and time types and specified `range` for each field (the values wrap around
  after second row):

```sql
CREATE TABLE times (
    dt DATE NOT NULL,
    ts TIMESTAMP NOT NULL,
    t TIME NOT NULL
) with (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 3,
            "rate": 1,
            "fields": {
                "ts": { "range": ["2024-08-28T00:00:00Z", "2024-08-28T00:00:02Z"], "scale": 1000 },
                "dt": { "range": ["2024-08-28", "2024-08-30"] },
                "t": { "range": ["00:00:05", "00:00:07"], "scale": 1000 }
            }
        }]
      }
    }
  }]'
);
```

Will generate the following data:

```text
+------------+---------------------+----------+
| dt         | ts                  | t        |
+------------+---------------------+----------+
| 2024-08-28 | 2024-08-28 00:00:00 | 00:00:05 |
| 2024-08-29 | 2024-08-28 00:00:01 | 00:00:06 |
| 2024-08-28 | 2024-08-28 00:00:00 | 00:00:05 |
+------------+---------------------+----------+
```

* A table with values provided inline in columnar format:

```sql
CREATE TABLE example1 (
    col1 INT NOT NULL,
    col2 VARCHAR NOT NULL
) WITH (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 4,
            "fields": {
                "col1": { "values": [1, 2, 3, 4] },
                "col2": { "values": ["a", "b", "c", "d"] }
            }
        }]
      }
    }
  }]'
);
```

Will generate the following data:

```text
+------+------+
| col1 | col2 |
+------+------+
| 1    | a    |
| 2    | b    |
| 3    | c    |
| 4    | d    |
+------+------+
```
