# Datagen input connector

Datagen is a source connector that generates synthetic data for testing,
prototyping and benchmarking purposes.

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

* `limit` - How many rows to generate. If not specified, the plan will run indefinitely.
* `rate` - How many rows to generate per second. If not specified, the plan will run as fast as possible. See also the `workers` parameter.
* `fields` - A map of field names to field generation options.

### Random Field Settings

Each field has a strategy that defines how a value is picked:

* `strategy` - The strategy to use for generating values. The following strategies are available:
    * `increment` - (default) Use an incrementing sequence of values. An optional scale factor can be used to skip values.
    * `uniform` - Generate random values from a uniform distribution.
    * `zipf` - Generate random values from a Zipf distribution. The exponent of the distribution can be set with the 
      `s` parameter.
    * `string` - Check the [String Generation](#string-generation-methods) section for details.

A field can have an optional `range` parameter that defines the range of values a strategy will pick from. The 
meaning of the range depends on the type:

- For integer/floating point types specifies min/max values.
  If not set, the generator will produce values for the entire range of the type for number types.
- For string/binary types specifies min/max length, values are required to be `>=0`.
  If not set, a range of `[0, 25)` is used by default.
- For timestamp types specifies the min/max in milliseconds from the number of non-leap
  milliseconds since January 1, 1970 0:00:00.000 UTC (aka “UNIX timestamp”).
  If not set, a range of `[0, 4102444800)` is used by default (1970-01-01 -- 2100-01-01).
- For time types specifies the min/max in milliseconds.
  If not set, the range is 24h. Range values are required to be `>=0`.
- For date types specifies the min/max in days from the number of days since January 1, 1970.
  If not set, a range of `[0, 54787)` is used by default (1970-01-01 -- 2100-01-01).
- For array types specifies the min/max number of elements it should contain.
  If not set, a range of `[0, 5)` is used by default. Range values are required to be >=0.
- For map types specifies the min/max number of key-value pairs it should contain.
  If not set, a range of `[0, 5)` is used by default.
- For struct/boolean/null types `range` is ignored.

The `values` parameter can be used to specify a list of values to pick from. If set, the `range` parameter is ignored. 

`null_percentage` adds a chance for generating `null` values for this field. If not specified, the field will 
never be `null`.

If the type of the field is a complex type (array, map, struct), the `value`, `key`/`value`, and `fields` parameter 
respectively define the field settings for the complex type.

#### String Generation Methods

In case the field type is a string, the `string` strategy can be used to generate strings.
The type of string is chosen by the `method` parameter. The following methods are available:

- Lorem: `word`, `words`, `sentence`, `sentences`, `paragraph`, `paragraphs`
- Name: `first_name`, `last_name`, `title`, `suffix`, `name`, `name_with_title`, `phone_number`, `cell_number`
- Internet: `domain_suffix`, `email`, `username`, `password`, `ipv4`, `ipv6`, `ip`, `mac_address`, `user_agent`
- Company: `company_suffix`, `company_name`, `buzzword`, `buzzword_middle`, `buzzword_tail`, `catch_phrase`, `bs_verb`, `bs_adj`, `bs_noun`, `bs`, `profession`, `industry`
- Currency: `currency_code`, `currency_name`, `currency_symbol`
- Finance: `credit_card_number`
- Address: `city_prefix`, `city_suffix`, `city_name`, `country_name`, `country_code`, `street_suffix`, `street_name`, `time_zone`, `state_name`, `state_abbr`, `secondary_address_type`, `secondary_address`, `zip_code`, `post_code`, `building_number`, `latitude`, `longitude`
- Barcode: `isbn10`, `isbn13`, `isbn`
- Files: `file_path`, `file_name`, `file_extension`, `dir_path`

For some of these parameters (`words`, `sentences`, `paragraphs`) the length is controlled by the `range` parameter.

## Example usage

For a tutorial on how to use the Datagen connector, see the
[Random Data Generation](../../tutorials/basics/part4) tutorial.
