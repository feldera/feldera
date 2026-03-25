---
categories: [datetime, string]
---

# to_date and date_format rewrites

Two related but distinct patterns:

- `to_date(str, fmt)` → `PARSE_DATE(strptime_fmt, str)`. Use `PARSE_DATE`, NOT
  `PARSE_TIMESTAMP` — `PARSE_TIMESTAMP` panics on date-only strings. Translate
  Java format codes to strftime: `yyyy` → `%Y`, `MM` → `%m`, `dd` → `%d`.
  No-format variant: `CAST(str AS DATE)`.

- `date_format(ts, fmt)` → more complex. `FORMAT_TIMESTAMP` does not exist in
  Feldera. `FORMAT_DATE` exists but always truncates time (casts input to DATE).
  For date-only formats use `FORMAT_DATE(strftime_fmt, CAST(d AS DATE))`.
  For formats containing time parts (`HH`, `mm`, `ss`) use CONCAT + FORMAT_DATE
  for the date portion + EXTRACT for each time component, zero-padding with
  CONCAT(REPEAT('0', ...), CAST(EXTRACT(...) AS VARCHAR)).

Spark:
```sql
CREATE TABLE raw_events (
  event_id   BIGINT,
  user_id    BIGINT,
  event_date VARCHAR,
  occurred_at TIMESTAMP
) USING parquet;

-- Parse a date string
SELECT
  event_id,
  to_date(event_date, 'yyyy-MM-dd')          AS parsed_date,
  date_format(occurred_at, 'yyyy-MM-dd')     AS day_label,
  date_format(occurred_at, 'yyyy-MM-dd HH:mm') AS minute_label
FROM raw_events;
```

Feldera:
```sql
CREATE TABLE raw_events (
  event_id    BIGINT,
  user_id     BIGINT,
  event_date  VARCHAR,
  occurred_at TIMESTAMP
);

-- NOTE: to_date → PARSE_DATE (NOT PARSE_TIMESTAMP).
-- NOTE: date_format with date-only fmt → FORMAT_DATE(strftime, CAST(ts AS DATE)).
-- NOTE: date_format with time parts → CONCAT + FORMAT_DATE + EXTRACT (no FORMAT_TIMESTAMP).
-- Zero-padding uses CONCAT(REPEAT('0', 2 - LENGTH(CAST(EXTRACT(...) AS VARCHAR))), ...).
SELECT
  event_id,
  PARSE_DATE('%Y-%m-%d', event_date)         AS parsed_date,
  FORMAT_DATE('%Y-%m-%d', CAST(occurred_at AS DATE)) AS day_label,
  CONCAT(
    FORMAT_DATE('%Y-%m-%d', CAST(occurred_at AS DATE)),
    ' ',
    CONCAT(REPEAT('0', 2 - LENGTH(CAST(EXTRACT(HOUR   FROM occurred_at) AS VARCHAR))),
           CAST(EXTRACT(HOUR   FROM occurred_at) AS VARCHAR)),
    ':',
    CONCAT(REPEAT('0', 2 - LENGTH(CAST(EXTRACT(MINUTE FROM occurred_at) AS VARCHAR))),
           CAST(EXTRACT(MINUTE FROM occurred_at) AS VARCHAR))
  ) AS minute_label
FROM raw_events;
```
