# Null output connector

The null output connector discards all output data.  It is useful when
only the side effects of a
[postprocessor](../../sql/postprocessors.md) matter, or in tests where
no persistent output destination is needed.

Unlike the [file output connector](file.md), the null output connector
works in any deployment, including cloud environments where the
pipeline runs in an isolated container without filesystem access.

## Example

The following discards output:

```sql
CREATE TABLE stocks (
    symbol VARCHAR NOT NULL,
    price DECIMAL(38, 2) NOT NULL
);
CREATE VIEW copy WITH (
  'connectors' = '[{
    "name": "copy",
    "transport": {
      "name": "null_output"
    },
    "format": { "name": "csv" }
  }]'
)
AS SELECT * FROM stocks;
```
