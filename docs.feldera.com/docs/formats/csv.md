# CSV Format

Feldera can ingest and output data in the CSV format
- via [`ingress` and `egress` REST endpoints](/tutorials/basics/part2) by specifying `?format=csv` in the URL
- as a payload received from or sent to a connector

Here we document the CSV format and how it interacts with different SQL types.

The CSV format expects comma-separated columns and rows
separated by a newline (`\n`). The egress and expected ingress character
encoding is UTF-8.

The rows must appear in the same order as the program table definition specified
the fields. For example, consider the following table:

```sql
create table git_commit (
    commit_id varchar not null,
    commit_date timestamp not null,
);
```

An example CSV row would look like this:

```csv
c6d16e61,2024-02-25 12:12:33
```

Any unspecified rows following are discarded on ingress: This line
`c6d16e61,2024-02-25 12:12:33,invalid` would be equivalent to the row above for
the given table.

For nullable types that are not strings, a value can be set to `NULL` by either
leaving the field empty, or writing `NULL` or `null` as the column value. For
string types (e.g., `CHAR`, `VARCHAR`), leaving the field empty will be
interpreted as an empty string instead of `NULL`.

## Types

| Type                                    | Example                                         |
|-----------------------------------------|-------------------------------------------------|
| BOOLEAN                                 | `true`, `false`                                 |
| TINYINT,SMALLINT, INTEGER, BIGINT       |  `1`, `-9`                                      |
| FLOAT, DOUBLE, DECIMAL                  | `-1.40`, `12.53`, `1e20`, `NaN`                 |
| CHAR, VARCHAR, STRING, TEXT             | `abc`                                           |
| TIME                                    | `12:12:33`, `23:59:29.483`, `23:59:09.483221092`|
| TIMESTAMP                               | `2024-02-25 12:12:33`                           |
| DATE                                    | `2024-02-25`                                    |
| BIGINT ARRAY                            | `[1, 2]`                                        |
| VARCHAR ARRAY ARRAY                     | `[[ 'abc', '123'], ['c', 'sql']]`               |
| VARIANT                                 | `"{""foo"": ""bar""}"` (see [`VARIANT` documetation](/sql/json))|

### `BOOLEAN`

The accepted values are `true` or `false`.

### Integers (`TINYINT`, `SMALLINT, `INTEGER`, `BIGINT`)

Must be a valid integer and fit the range of the type (see [SQL
Types](../sql/types.md)), otherwise an error is returned on ingress.

### Decimals (`DECIMAL` / `NUMERIC`)

Either scientific notation (e.g., `3e234`) or standard floating point numbers
are valid `1.23`. The provided value must fit within the specified range or
precision, otherwise an error is returned.

### Floating point numbers (`FLOAT`, `DOUBLE`)

Either scientific notation (e.g., `3e234`), or standard floating point numbers
are valid `1.23`.

Not a number can be specified using `NaN` (case insensitive). Infinity
can be specified using `-Inf` or `Inf`, or `+Inf` (case insensitive).
If a floating point value is provided that is outside of the valid range
for that type, it is set to `NaN`.

If a value is provided inside the maximum range of the type but still
can't be represented by the type it is rounded to the nearest representable
floating point value.

### Strings (`CHAR`, `VARCHAR`, `TEXT`, `STRING`)

Accepts strings with any number of characters.

:::note

The CSV parser does not currently enforce limits on the number
of characters in a string.  Strings that exceed the length
specified in the SQL table declaration are ingested
without truncation.

:::

If a string contains commas, it can be quoted with `"`: `"string, with, commas"`.

Note that a string of just `null` or `NULL` for a nullable column gets
translated to the `NULL` value in SQL.

### `TIME`

Specifies times using the `HH:MM:SS.fffffffff` format where:

- `HH` is hours from `00-23`.
- `MM` is minutes from `00-59`.
- `SS` is seconds from `00-59`.
- `fffffffff` is the sub-second precision up to 9 digits from `0` to `999999999`

A leading 0 can be skipped in hour, minutes and seconds. Specifying the
subsecond precision is optional and can have any number of digits from 0 to 9.
Leading and trailing whitespaces are ignored for ingress.

### `DATE`

Specifies dates using the `YYYY-MM-DD` format.

- `YYYY` is the year from `0001-9999`
- `MM` is the month from `01-12`
- `DD` is the day from `01-31`

Invalid dates (e.g., `1997-02-29`) are rejected with an error during ingress.
Leading zeros can be skipped, e.g., `0001-1-01`, `1-1-1`, `0000-1-1` are all
equal and valid. Leading and trailing whitespaces are ignored for ingress.

### `TIMESTAMP`

Specifies dates using the `YYYY-MM-DD HH:MM:SS.fff` format.

- `YYYY` is the year from `0001-9999`
- `MM` is the month from `01-12`
- `DD` is the day from `01-31`
- `HH` is hours from `00-23`.
- `MM` is minutes from `00-59`.
- `SS` is seconds from `00-59`.
- `fff` is the sub-second precision up to 3 digits from `0` to `999`

Note that the same rules as specified in the Date and Time sections apply,
except that the sub-second precision is limited to three digits (microseconds).
Specifying more digits for the subsecond precision on ingress will trim the
fraction to microseconds. Leading and trailing whitespaces are ignored
for ingress.

### `ARRAY`

The CSV format does not have native support for arrays. Arrays are expected to
be represented in the form of a string that is a valid JSON array. e.g., a value
for `ARRAY BIGINT` can be expressed as `'[1,2,3]'`.

## Configuring CSV event streams

### Configure connectors

When adding a new input or output connector on a table or view,
the data format is specified in the `format` field of the connector configuration:

```sql
create table FAILED_BANKS (
  name varchar,
  city varchar,
  state varchar,
  cert bigint,
  acquirer varchar,
  closing varchar,   -- needs to be translated from 'DD-MMM-YY' format
  fund bigint
) with (
  'connectors' = '[{
    "transport": {
        "name": "url_input",
        "config": { "path": "https://www.fdic.gov/system/files/2024-07/banklist.csv" }
    },
    "format": {
        "name": "csv",
        "config": { "headers": true }
    }
}]'
);
```

- `delimiter`: A single character that delimits fields. The default is `","`.
- `headers`: Whether the first line of input is a header line. If this is set to true, Feldera ignores the first line. This applies only to input connectors (Feldera never writes a header line for CSV output). The default is `false`.
