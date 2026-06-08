-- rule: negative_substr
-- spark: substring(str, -n) — negative position counts from end of string in Spark (e.g. substring('Spark SQL', -3) → 'SQL')
-- feldera: UNSUPPORTED — Feldera does not support negative positions in SUBSTRING; returns the full string or wrong result. Mark unsupported when position argument may be negative.
CREATE OR REPLACE TEMP VIEW product_view_v1 AS SELECT id, name, substring(name, -4) AS last_four FROM product_names WHERE id > 0;
