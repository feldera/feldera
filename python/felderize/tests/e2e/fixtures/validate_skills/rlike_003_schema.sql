-- rule: rlike
-- spark: RLIKE(s, pattern) / s RLIKE pattern — regex match returning Boolean
-- feldera: RLIKE(s, pattern) — same function and infix form, supported directly in Feldera. CRITICAL: Use only simple regex patterns without backslash escapes (e.g. use [.] instead of \\., use [+] instead of \\+, use [0-9] instead of \\d). Feldera SQL string literals do not apply Spark/Java backslash escaping, so \\. in a Spark pattern means literal-backslash-then-any in Feldera.
CREATE TABLE product_sku (
  sku_id INT,
  sku_code STRING,
  category STRING
);
