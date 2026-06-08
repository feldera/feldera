-- rule: monotonically_increasing_id
-- spark: monotonically_increasing_id() — generates a unique, monotonically increasing 64-bit integer per row using partition metadata; nondeterministic across runs
-- feldera: UNSUPPORTED — Feldera has no equivalent; no stable row identity in streaming mode. Mark unsupported and suggest a surrogate key from existing columns or a sequence.
CREATE OR REPLACE TEMP VIEW event_ids_v1 AS
SELECT
  monotonically_increasing_id() AS unique_id,
  event_id,
  event_name,
  event_time
FROM events_log
ORDER BY event_id;
