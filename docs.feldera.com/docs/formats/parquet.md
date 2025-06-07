# Parquet Format

Feldera can ingest and output data in the [Parquet format](https://parquet.apache.org/).

- via [`ingress` and `egress` REST endpoints](/tutorials/basics/part2) by specifying `?format=parquet` in the URL
- as a payload received from or sent to a connector

We document the Parquet format and how it interacts with different SQL types in this page.

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
| `BINARY`, `VARBINARY`                      | `DataType::Binary`                                                                 |
| `TIME`                                     | `DataType::UInt64` (time in nanoseconds)                                           |
| `TIMESTAMP`                                | `DataType::Timestamp(TimeUnit::Millisecond, None)` (milliseconds since unix epoch) |
| `DATE`                                     | `DataType::Int32` (days since unix epoch)|
| `ARRAY`                                    | `DataType::LargeList`                                                              |
| `STRUCT`                                   | `DataType::Struct`                                                                 |
| `MAP`                                      | `DataType::Dictionary`                                                             |
| `VARIANT`                                  | `LargeUtf8` (JSON-encoded string, see [VARIANT documentation](/sql/json))   |


## Example

In this example, we configure a table to load data from a Parquet file.

```sql
create table PARTS (
  part bigint not null,
  vendor bigint not null,
  price bigint not null
) with ('connectors' = '[{
  "transport": {
    "name": "url_input",
    "config": { "path": "https://feldera-basics-tutorial.s3.amazonaws.com/parts.parquet" }
  },
  "format": {
    "name": "parquet",
    "config": {}
  }
}]');
```

For reference, the following python script was used to generate the `parts.parquet` file:

```python
import pyarrow as pa
import pyarrow.parquet as pq

data = {
    'PART': [1, 2, 3],
    'VENDOR': [2, 1, 3],
    'PRICE': [10000, 15000, 9000]
}
table = pa.Table.from_pydict(data)
pq.write_table(table, 'parts.parquet')
```
