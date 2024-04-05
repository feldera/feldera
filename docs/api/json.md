# JSON Format

Feldera can ingest and output data in the JSON format
- via [`/ingress`](/api/push-data-to-a-sql-table) and
  [`/egress`](/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)
  REST endpoints, or
- as a payload received from or sent to a connector

Here we document the JSON format supported by Feldera.  The specification
consists of four parts:

1. [Encoding invividual table rows](#encoding-individual-rows). Describes
   JSON encoding of an individual row in a SQL table or view, e.g.:
   ```json
   {"part": 1, "vendor": 2, "price": 10000}
   ```
2. [Encoding data change events](#encoding-data-change-events).
   A data change event represents an insertion, deletion, or modification
   of a single row, e.g.:
   ```json
   {"insert": {"part": 1, "vendor": 2, "price": 30000}}
   ```
3. [Encoding multiple changes](#encoding-multiple-changes). Describes JSON
   encoding of a stream of data change events, e.g.:
   ```json
   {"delete": {"part": 1, "vendor": 2, "price": 10000}}
   {"insert": {"part": 1, "vendor": 2, "price": 30000}}
   {"insert": {"part": 2, "vendor": 3, "price": 34000}}
   ```
4. [Configuring JSON event streams](#configuring-json-event-streams).
   Describes how the user can specify the JSON format for a stream of events
   when sending and receiving data via connectors or over HTTP.

## Encoding individual rows

A row in a SQL table or view is encoded as a JSON object whose keys match
the names of the columns in the table.  Keys can occur in an arbitrary order.
Column names are case-insensitive, except for columns whose names are declared
in double quotes (e.g., `create table foo("col")`).  `NULL`s are encoded
as JSON `null` values or by simply omitting the columns whose value is `NULL`.

For example, given the following table declaration

```sql
create table json_test (
    b BOOLEAN,
    i INTEGER,
    d DOUBLE,
    v VARCHAR(32),
    cc CHAR(16),
    t TIME,
    ts TIMESTAMP,
    dt DATE,
    ar BIGINT ARRAY
);
```

the JSON encoding of a row would look like this:

```json
{
    "B":true,
    "I":-1625240816,
    "D":0.7879946935782574,
    "V":"quod",
    "CC":"voluptatem",
    "T":"05:05:24",
    "TS":"2023-11-21 23:19:09",
    "DT":"2495-03-07",
    "AR":[1,2,3,4,5]
}
```

## Types

| Type                                    | Example                                                 |
|-----------------------------------------|---------------------------------------------------------|
| BOOLEAN                                 | `true`, `false`                                         |
| TINYINT,SMALLINT, INTEGER, BIGINT       |  `1`, `-9`                                              |
| FLOAT, DOUBLE, DECIMAL                  | `-1.40`, `"-1.40"`, `12.53`, `"12.53"`, `1e20`, `"1e20"`|
| VARCHAR, CHAR, STRING                   | `abc`                                                   |
| TIME                                    | `12:12:33`, `23:59:29.483`, `23:59:09.483221092`        |
| TIMESTAMP                               | `2024-02-25 12:12:33`                                   |
| DATE                                    | `2024-02-25`                                            |
| BIGINT ARRAY                            | `[1, 2]`                                                |
| VARCHAR ARRAY ARRAY                     | `[[ 'abc', '123'], ['c', 'sql']]`                       |

### `BOOLEAN`

The accepted values are `true` or `false`.

### Integers (`TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`)

Must be a valid integer and fit the range of the type (see [SQL
Types](../sql/types.md)), otherwise an error is returned on ingress.

### Decimals (`DECIMAL` / `NUMERIC`)

Both the scientific notation (e.g., `3e234`) and standard floating point numbers
(`1.23`) are valid. The parser will accept decimals formatted as JSON numbers
(`1.234`) or strings (`"1.234"`).  The latter representation is more robust as it
avoids loss of precision during parsing (the Rust JSON parser we use represents all
fractional numbers as 64-bit floating point numbers internally,  which can cause loss
of precision for decimal numbers that cannot be accurately represented in that way).

### Floating point numbers (`FLOAT`, `DOUBLE`)

Both the scientific notation (e.g., `3e234`) and standard floating point numbers
(`1.23`) are valid.

:::note

`NaN`, `Inf`, and `-Inf` floating point values are currently not supported by
the JSON parser and encoder.

:::

### Strings (`CHAR`, `VARCHAR`, `STRING`, `TEXT`)

SQL strings are encoded as JSON strings.

:::note

The JSON parser does not currently enforce limits on the number
of characters in a string.  Strings that exceed the length
specified in the SQL table declaration are ingested
without truncation.

:::

### `TIME`

Specifies times using the `HH:MM:SS.fffffffff` format where:

- `HH` is hours from `00-23`.
- `MM` is minutes from `00-59`.
- `SS` is seconds from `00-59`.
- `fffffffff` is the sub-second precision up to 9 digits from `0` to `999999999`

A leading 0 can be skipped in hours, minutes and seconds. Specifying the
subsecond precision is optional and can have any number of digits from 0 to 9.
Leading and trailing whitespaces are ignored.

### `DATE`

Specifies dates using the `YYYY-MM-DD` format.

- `YYYY` is the year from `0001-9999`
- `MM` is the month from `01-12`
- `DD` is the day from `01-31`

Invalid dates (e.g., `1997-02-29`) are rejected with an error during ingress.
Leading zeros are skipped, e.g., `0001-1-01`, `1-1-1`, `0000-1-1` are all
equal and valid. Leading and trailing whitespaces are ignored.

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
fraction to microseconds. Leading and trailing whitespaces are ignored.

### `ARRAY`

Arrays are encoded as JSON arrays.

## Encoding data change events

Feldera operates over streams of **data change events**.
A data change event represents an insertion, deletion, or modification of a
single row in a SQL table or view.  We currently support two
data change event formats in JSON: (1) the raw format and (2) the insert/delete format.

### The insert/delete format

A data change event in this format is represented as a JSON object with a
single key, which must be equal to `insert` or `delete`.  The associated value
represents the table row to be inserted or deleted, encoded using the format
documented above.  Example row insertion event:

```json
{"insert": {"part": 1, "vendor": 2, "price": 30000}}
```

Example row deletion event:

```json
{"delete": {"part": 1, "vendor": 2, "price": 10000}}
```

### The raw format

This format is applicable to append-only event streams where rows can only
be inserted but not deleted.  In this case, a data change event can be
represented as a SQL record without any additional framing, with the insert
operation being implied, e.g., the following data change event in the raw
format

```json
{"part": 1, "vendor": 2, "price": 30000}
```

is equivalent to

```json
{"insert": {"part": 1, "vendor": 2, "price": 30000}}
```

## Encoding multiple changes

Data change events are exchanged as data streams transmitted over transports such
as HTTP or Kafka.  We use newline-delimited JSON (NDJSON) to assemble individual
data change events into a stream.  In NDJSON, each line within the data stream
represents a self-contained and valid JSON document, in this case, a data change
event:

```json
{"delete": {"part": 1, "vendor": 2, "price": 10000}}
{"insert": {"part": 1, "vendor": 2, "price": 30000}}
{"insert": {"part": 2, "vendor": 3, "price": 34000}}
...
```

A stream of data change events can be [configured](#configuring-json-event-streams)
to combine multiple events into JSON arrays.  This format still uses NDJSON,
where each line contains an array of data change events:

```json
[{"delete": {"part": 1, "vendor": 2, "price": 10000}}, {"insert": {"part": 1, "vendor": 2, "price": 30000}}]
[{"insert": {"part": 2, "vendor": 3, "price": 34000}}, {"delete": {"part": 3, "vendor": 1, "price": 5000}}]
...
```

This format allows one to break up the event stream into transport
messages so that each message contains a valid JSON document
by encoding all events in the message as an array.

## Configuring JSON event streams

### Configure connectors via the Feldera Web Console

When creating a new input or output connector using the Feldera Web Console,
the data format is configured in the `FORMAT` section of the connector
creation wizard:

1. choose `JSON` from the list of supported formats
1. choose appropriate data change event encoding ([`Raw`](#the-raw-format) or
   [`Insert & Delete`](#the-insertdelete-format))
1. enable array encapsulation if the stream contains events grouped in
   [arrays](#encoding-multiple-changes)

![Data change event format selector](format_selector.png)

See also the [input/output connector tutorial](tutorials/basics/part3.md).

### Configure connectors via the REST API

When creating or modifying connectors via the REST API
[`/connectors` endpoint](/api/create-a-new-connector),
the data format is specified in the `format` field of
the connector configuration:

```json
{
    "transport": {
        "name": "url_input",
        "config": {
            "path":"https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
        }
    },
    "format": {
        // Choose JSON format for the connector.
        "name": "json",

        "config": {
            // Choose data change event format for this connector.
            // Supported values are `insert_delete` and `raw`.
            "update_format": "insert_delete",

            // Disable array encoding.
            "array": false
        }
    },
    "max_queued_records":1000000
}
```

### Streaming JSON over HTTP

When sending data to a pipeline over HTTP via the [`/ingress`](/api/push-data-to-a-sql-table)
API endpoint, the data format is specified as part of the URL, e.g.:

```bash
# `?format=json` - Chooses JSON format for the stream.
# `&update_format=insert_delete` - Specifies data change event format. Supported values are `insert_delete` and `raw`.
# `&array=false` - Don't use array encoding.
curl -X 'POST' 'http://localhost:8080/v0/pipelines/018a67a5-32e8-7e23-825d-a8a64872ab7c/ingress/PART?format=json&update_format=insert_delete&array=false' -d '
{"insert": {"id": 1, "name": "Flux Capacitor"}}
{"insert": {"id": 2, "name": "Warp Core"}}
{"insert": {"id": 3, "name": "Kyber Crystal"}}'
```

When receiving data from a pipeline over HTTP via the
[`/egress`](/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table)
API endpoint, we currently only support the insert/delete data change event
format with array encapsulation.  Specify `?format=json` in the URL
to choose this encoding for output data.

```bash
curl -s -N -X 'POST' http://localhost:8080/v0/pipelines/018a67a5-32e8-7e23-825d-a8a64872ab7c/egress/PREFERRED_VENDOR?format=json
```

See also the [HTTP input and output tutorial](tutorials/basics/part3.md).
