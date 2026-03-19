-- Demo: JSON parsing and VARIANT field access
-- Covers: get_json_object, from_json, json_tuple → PARSE_JSON + bracket access

CREATE TABLE IF NOT EXISTS raw_events (
  event_id STRING NOT NULL,
  event_type STRING,
  payload STRING,
  occurred_at TIMESTAMP,
  CONSTRAINT raw_events_pk PRIMARY KEY (event_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS user_profiles (
  user_id STRING NOT NULL,
  metadata STRING,
  CONSTRAINT user_profiles_pk PRIMARY KEY (user_id)
)
USING DELTA;

-- Extract typed fields from a JSON payload column
CREATE OR REPLACE TEMP VIEW parsed_events AS
SELECT
  event_id,
  event_type,
  occurred_at,
  get_json_object(payload, '$.user_id')     AS user_id,
  get_json_object(payload, '$.amount')      AS amount_str,
  CAST(get_json_object(payload, '$.amount') AS DOUBLE) AS amount,
  get_json_object(payload, '$.currency')    AS currency,
  get_json_object(payload, '$.items[0]')    AS first_item
FROM raw_events;

-- Aggregate per user, parsing nested JSON
CREATE OR REPLACE TEMP VIEW user_event_summary AS
SELECT
  get_json_object(payload, '$.user_id') AS user_id,
  COUNT(*)                              AS event_count,
  SUM(CAST(get_json_object(payload, '$.amount') AS DOUBLE)) AS total_amount,
  MIN(occurred_at)                      AS first_event,
  MAX(occurred_at)                      AS last_event
FROM raw_events
WHERE event_type = 'purchase'
GROUP BY get_json_object(payload, '$.user_id');
