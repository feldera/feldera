# Empty input connector

The empty input connector produces no data.  It immediately signals
end of input when the pipeline starts.

This connector can be useful for testing and debugging.

## Example

```sql
CREATE TABLE t (id INT NOT NULL) WITH (
    'connectors' = '[{
        "name": "t",
        "transport": {"name": "empty_input"},
        "format": {"name": "json"}
    }]'
);
```
