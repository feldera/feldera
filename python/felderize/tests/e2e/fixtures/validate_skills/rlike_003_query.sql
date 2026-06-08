-- rule: rlike
-- spark: RLIKE(s, pattern) / s RLIKE pattern — regex match returning Boolean
-- feldera: RLIKE(s, pattern) — same function and infix form, supported directly in Feldera. CRITICAL: Use only simple regex patterns without backslash escapes (e.g. use [.] instead of \\., use [+] instead of \\+, use [0-9] instead of \\d). Feldera SQL string literals do not apply Spark/Java backslash escaping, so \\. in a Spark pattern means literal-backslash-then-any in Feldera.
CREATE OR REPLACE TEMP VIEW sku_classification_v3 AS SELECT sku_id, sku_code, category, sku_code RLIKE '^[A-Z]{3}[0-9]{4}[A-Z]$' AS matches_format_a, sku_code RLIKE '^[A-Z0-9]{5,8}$' AS is_alphanumeric FROM product_sku WHERE sku_code RLIKE '[A-Z0-9]';
