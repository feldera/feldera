# File output connector

The file output connector writes a file local to the machine that runs
the pipeline.  The file output connector is unlikely to be useful in a
Kubernetes-based cloud deployment, where the pipeline runs in an
isolated container, but it can sometimes be valuable outside of these
environments.

## File output connector configuration

The file connector has the following configuration parameters:

* `path`: The file to write, specified as an absolute filename.
  Required.

## Example

The following writes a file named `/tmp/output.txt`:

```sql
CREATE TABLE stocks (
    symbol VARCHAR NOT NULL,
    price DECIMAL(38, 2) NOT NULL
);
CREATE VIEW copy WITH (
  'connectors' = '[{
    "transport": {
      "name": "file_output",
      "config": {
        "path": "/tmp/output.txt"
      }
    }
  }]'
)
AS SELECT * FROM Stocks;
```
