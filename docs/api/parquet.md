# Parquet Format

:::caution Under Construction

Note that the Parquet support is currently under development and is missing
some functionality such as support for arrays and does not propagate information
about deletes.

Feldera can ingest and output data in the [Parquet format](https://parquet.apache.org/).

- via [`ingress` and `egress` REST endpoints](/docs/tutorials/basics/part2) by specifying `?format=parquet` in the URL
- as a payload received from or sent to a connector

Here we document the Parquet format and how it interacts with different SQL types.

## Types

The parquet file is expected to be a valid parquet file with a schema. The schema
(row name and type) must match the table definition in the Feldera pipeline program. We
use [Arrow](https://arrow.apache.org/) to specify the data-types in parquet. The following table
shows the mapping between Feldera SQL types
and [Arrow types](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html).

| Feldera SQL Type                           | Apache Arrow Type                                                                  |
|--------------------------------------------|------------------------------------------------------------------------------------|
| `BOOLEAN`                                  | `Boolean`                                                                          |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `Int8`, `Int16`, `Int32`, `Int64`                                                  |
| `FLOAT`, `DOUBLE`, `DECIMAL`               | `Float32`, `Float64`, `Decimal`                                                    |
| `VARCHAR`, `CHAR`, `STRING`                | `LargeUtf8`                                                                        |
| `TIME`                                     | `DataType::UInt64` (time in nanoseconds)                                           |
| `TIMESTAMP`                                | `DataType::Timestamp(TimeUnit::Millisecond, None)` (milliseconds since unix epoch) |
| `DATE`                                     | `DataType::Int32` (days since unix epoch)                                          |
| `BIGINT ARRAY`                             | TBD                                                                                |
| `VARCHAR ARRAY ARRAY`                      | TBD                                                                                |
