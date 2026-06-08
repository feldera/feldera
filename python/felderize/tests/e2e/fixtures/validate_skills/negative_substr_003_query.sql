-- rule: negative_substr
-- spark: substring(str, -n) — negative position counts from end of string in Spark (e.g. substring('Spark SQL', -3) → 'SQL')
-- feldera: UNSUPPORTED — Feldera does not support negative positions in SUBSTRING; returns the full string or wrong result. Mark unsupported when position argument may be negative.
CREATE OR REPLACE TEMP VIEW logs_view_v3 AS SELECT id, message, substring(message, -8) AS error_code FROM log_messages WHERE id <= 10;
