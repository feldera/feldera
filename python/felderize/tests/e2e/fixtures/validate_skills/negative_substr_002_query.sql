-- rule: negative_substr
-- spark: substring(str, -n) — negative position counts from end of string in Spark (e.g. substring('Spark SQL', -3) → 'SQL')
-- feldera: UNSUPPORTED — Feldera does not support negative positions in SUBSTRING; returns the full string or wrong result. Mark unsupported when position argument may be negative.
CREATE OR REPLACE TEMP VIEW domain_view_v2 AS SELECT id, domain, substring(domain, -3) AS tld FROM domain_suffixes WHERE length(domain) >= 3;
