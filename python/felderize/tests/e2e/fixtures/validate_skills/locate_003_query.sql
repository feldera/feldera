-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW error_analysis AS SELECT
  log_id,
  message,
  LOCATE('ERROR', message) AS error_pos,
  LOCATE('WARN', message) AS warn_pos,
  LOCATE('[', message) AS bracket_pos
FROM log_entries;
