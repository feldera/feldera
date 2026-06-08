-- rule: concat_concat_ws
-- spark: CONCAT(s1, s2, ...) — concatenate strings; CONCAT_WS(sep, s1, s2, ...) — concatenate with separator
-- feldera: CONCAT(s1, s2, ...) / CONCAT_WS(sep, s1, s2, ...) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW event_summaries_v3 AS
SELECT
  event_id,
  CONCAT(year, '-', month, '-', day) AS date_str,
  CONCAT_WS('|', action, status, CAST(event_id AS STRING)) AS event_summary
FROM event_logs
WHERE event_id > 0;
