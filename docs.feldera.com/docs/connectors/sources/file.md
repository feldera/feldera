# File input connector

The file input connector reads a file local to the machine that runs the
pipeline.  The file input connector is unlikely to be useful in a
Kubernetes-based cloud deployment, where the pipeline runs in an isolated
container, but it can sometimes be valuable outside of these environments.

The file input connector supports [fault
tolerance](/pipelines/fault-tolerance).

## File input connector configuration

The file connector has the following configuration parameters:

* `path`: The file to read, which may be specified as a filename or a `file://`
  URL.  In either case, it should use an absolute path.  Required.

* `buffer_size_byte`: The number of bytes of the file to read at one time.
  Optional.

* `follow`: If false (the default), the file input connector will read the file
  to its end and then stop.  If true, after reading the file's initial
  contents, the connector will watch for additional data appended to the file
  and continue to read it.

## Example

The following reads a file named `/tmp/input.txt`:

```sql
CREATE TABLE stocks (
    symbol VARCHAR NOT NULL,
    price DECIMAL(38, 2) NOT NULL
) with (
  'connectors' = '[{
    "transport": {
      "name": "file_input",
      "config": {
        "path": "/tmp/input.txt"
      }
    }
  }]'
);
```
