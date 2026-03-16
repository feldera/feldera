---
name: type-converter
description: Converts Spark SQL DDL to Feldera-compatible DDL. Covers type mappings (STRING→VARCHAR), DDL clause removal (USING parquet, TEMP VIEW), and schema-level rewrites.
---

# Type Converter

## Type Mappings

| Spark | Feldera | Notes |
|-------|---------|-------|
| `STRING` | `VARCHAR` | |
| `TEXT` | `VARCHAR` | |
| `INT` / `INTEGER` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `BOOLEAN` | `BOOLEAN` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `FLOAT` | `FLOAT` | Same |
| `DOUBLE` | `DOUBLE` | Same |
| `DATE` | `DATE` | Same |
| `TIMESTAMP` | `TIMESTAMP` | Same |
| `MAP<K, V>` | `MAP<K, V>` | Translate inner types (e.g. STRING→VARCHAR) |
| `ARRAY<T>` | `T ARRAY` | See array-types skill |
| `STRUCT<a: T, b: U>` | `ROW(a T, b U)` | Spark STRUCT becomes Feldera ROW |

## DDL Rewrites

| Spark syntax | Action |
|-------------|--------|
| `CREATE OR REPLACE TEMP VIEW` | → `CREATE VIEW` |
| `CREATE TEMPORARY VIEW` | → `CREATE VIEW` |
| `USING parquet` / `delta` / `csv` | Remove clause |
| `PARTITIONED BY (...)` | Remove clause |
## Examples

```sql
-- Spark
CREATE TABLE t (id BIGINT, name STRING, tags ARRAY<STRING>) USING parquet;
-- Feldera
CREATE TABLE t (id BIGINT, name VARCHAR, tags VARCHAR ARRAY);
```

```sql
-- Spark
CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM t ORDER BY id LIMIT 10;
-- Feldera
CREATE VIEW v AS SELECT * FROM t ORDER BY id LIMIT 10;
```
