-- Demo: to_date and date_format patterns
-- Covers: date string parsing, date-only formatting, time component formatting

CREATE TABLE raw_events (
  event_id    BIGINT,
  user_id     BIGINT,
  event_date  STRING,
  occurred_at TIMESTAMP
) USING parquet;

CREATE OR REPLACE TEMP VIEW event_labels AS
SELECT
  event_id,
  to_date(event_date, 'yyyy-MM-dd')            AS parsed_date,
  date_format(occurred_at, 'yyyy-MM-dd')       AS day_label,
  date_format(occurred_at, 'yyyy-MM-dd HH:mm') AS minute_label
FROM raw_events;
