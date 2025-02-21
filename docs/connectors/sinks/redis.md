# Redis output connector

:::caution Experimental feature
Redis support is an experimental feature of Feldera.
:::

Feldera can output data from a SQL table or view to Redis.

- The Redis connector uses the Rust [redis](https://docs.rs/redis/latest/redis/)
  crate.
- The Redis connector commits transactions to the given Redis instance.
- For every insertion in the view, this connector `SET`s the `key` with
  a JSON serialized string representing the values in this row.
- For every deletion to the view, this connector performs a `DEL` operation
  on the `key`.

:::warning
You must ensure that `key` is unique, or else it will be overridden.
:::

## Configuration

### Required Transport Parameters

* `connection_string` - Redis connection string.
  The connection string follows the following format:
  `redis://[<username>][:<password>@]<hostname>[:port][/[<db>][?protocol=<protocol>]]`
  This is parsed by the `redis` crate
  (See docs: [Connection Parameters](https://docs.rs/redis/latest/redis/#connection-parameters)).

### Optional Transport Parameters
* `key_separator` - Separator used to join multiple components into a single key.
  `:` by default.

### Format parameters

> Currently, only the `json` format is supported.

* `key_fields` - A **list** of columns used to form the `key` used in Redis.

## Example

Consider a Feldera pipeline with table `t0` and view `v0` as defined
below.

```sql
create table t0 (c0 int, c1 int, c2 varchar);

create materialized view v0 with (
'connectors' = '[
  {
    "transport": {
      "name": "redis_output",
      "config": {
        "connection_string": "redis://localhost:6379/0",
        "key_separator": ":"
      }
    },
    "format": {
        "name": "json",
        "config": {
          "key_fields": ["c0","c2"]
        }
    }
  }
]'
) as select * from t0;
```

We populate this table with an ad-hoc query as follows:

```sql
INSERT INTO t0 VALUES (1, 1, 'first')
```

The view `v0` will select this row from the table `t0`. This will be pushed to
redis as follows:

Key: `1:first`
Value: `"{\"c0\":1,\"c1\":1,\"c2\":\"first\"}\n"`

### Key

The key is formed by combining the values of the columns specified in the
connector definition via `key_fields`. You may specify a separator used to
form this key by defining the `key_separator` field in the connector definition.

```json
"config": {
  "key_fields": ["c0", "c1"],
  "key_separator": ":"
}
```

The key will be as follows: `c0:c1`

