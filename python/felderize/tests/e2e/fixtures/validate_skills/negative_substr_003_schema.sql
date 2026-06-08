-- rule: negative_substr
-- spark: substring(str, -n) — negative position counts from end of string in Spark (e.g. substring('Spark SQL', -3) → 'SQL')
-- feldera: UNSUPPORTED — Feldera does not support negative positions in SUBSTRING; returns the full string or wrong result. Mark unsupported when position argument may be negative.
CREATE TABLE log_messages (id INT, message STRING);
