# Null output connector

The null output connector discards all output data.

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
