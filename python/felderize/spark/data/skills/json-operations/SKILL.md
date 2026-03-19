---
name: json-operations
description: Translates Spark JSON functions to Feldera. Covers get_json_object, from_json, json_tuple, PARSE_JSON, VARIANT access, and TO_JSON.
---

# JSON Operations

## Purpose

Translate Spark JSON and semi-structured data patterns to Feldera's `VARIANT` model.

## Feldera JSON Model

| Function | Purpose |
|----------|---------|
| `PARSE_JSON(string)` | Convert JSON string → `VARIANT` |
| `TO_JSON(variant)` | Convert `VARIANT` → JSON string |
| `CAST(variant AS type)` | Extract typed value from VARIANT |

### VARIANT Access

| Pattern | Syntax | Example |
|---------|--------|---------|
| Object field | `variant['key']` | `v['name']` |
| Nested field | Chain brackets | `v['user']['id']` |
| Array element | `variant[index]` | `v[0]` |
| Dot syntax | `variant.field` | `v.name` |
| Case-sensitive | Quote field name | `v."lastName"` |

Results are always `VARIANT`. Cast to get a concrete type: `CAST(v['age'] AS INT)`.

## Spark → Feldera Translations

### get_json_object

```sql
-- Spark
get_json_object(json_col, '$.user.name')

-- Feldera
CAST(PARSE_JSON(json_col)['user']['name'] AS VARCHAR)
```

Rules:
- `$.a.b.c` → `PARSE_JSON(col)['a']['b']['c']`
- `$.items[0]` → `PARSE_JSON(col)['items'][0]`
- Always CAST the result to the expected type.
- If the column is already `VARIANT`, skip `PARSE_JSON`.

### from_json

Spark's `from_json(json_string, schema)` parses JSON into a struct.

Preferred approach — use `PARSE_JSON` + bracket access to extract individual fields:

```sql
-- Spark
from_json(payload, 'customer STRUCT<id: BIGINT, tier: STRING>, amount DECIMAL(12,2)')

-- Feldera
PARSE_JSON(payload)['customer']['id']    -- access nested fields
PARSE_JSON(payload)['amount']            -- access top-level fields
```

Full example:
```sql
-- Spark
SELECT order_id, from_json(payload, 'customer STRUCT<id: BIGINT, tier: STRING>').customer.id AS cust_id
FROM orders

-- Feldera
SELECT order_id, CAST(PARSE_JSON(payload)['customer']['id'] AS BIGINT) AS cust_id
FROM orders
```

### json_tuple

Spark's `json_tuple(json_string, key1, key2, ...)` extracts multiple keys.

```sql
-- Spark
SELECT j.k1, j.k2 FROM data LATERAL VIEW json_tuple(json_col, 'key1', 'key2') j AS k1, k2

-- Feldera (parse once, reuse alias)
SELECT
  PARSE_JSON(json_col) AS parsed,
  CAST(parsed['key1'] AS VARCHAR) AS k1,
  CAST(parsed['key2'] AS VARCHAR) AS k2
FROM data
```

### to_json

Direct equivalent:
```sql
-- Spark
to_json(struct_col)

-- Feldera
TO_JSON(struct_col)
```

### json_array_length

```sql
-- Spark
json_array_length(json_col)

-- Feldera
CARDINALITY(CAST(PARSE_JSON(json_col) AS VARIANT ARRAY))
```

## Unsupported JSON Functions

- `json_object_keys` — no direct equivalent
- `schema_of_json` — schema inference not available

## Notes

- Do NOT mark `get_json_object`, `from_json`, `json_tuple`, or `json_array_length` as unsupported — all have rewrites.
- Parse once, reuse: `SELECT PARSE_JSON(col) AS v, CAST(v['key'] AS VARCHAR) AS k` — Feldera supports lateral column aliases.
- For performance-critical paths, `CREATE TYPE` + `jsonstring_as_<type>` is more efficient than `PARSE_JSON` + bracket access.
