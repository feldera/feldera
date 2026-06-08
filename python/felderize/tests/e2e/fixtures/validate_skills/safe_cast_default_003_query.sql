-- rule: safe_cast_default
-- spark: TRY_CAST(invalid_str AS DOUBLE) / TRY_CAST(invalid_str AS BOOLEAN) — Spark returns NULL on failure; Feldera SAFE_CAST returns type default (0.0 for DOUBLE, false for BOOLEAN) instead of NULL
-- feldera: UNSUPPORTED — SAFE_CAST on REAL/DOUBLE/BOOLEAN targets does not return NULL on failure in Feldera; it returns the type default instead. No exact NULL-on-failure equivalent exists. Mark unsupported when NULL-on-failure semantics matter.
CREATE OR REPLACE TEMP VIEW numeric_parse_v3 AS SELECT id, category, TRY_CAST(num_text AS DOUBLE) AS cast_result FROM numeric_strings_3 WHERE category IN ('valid', 'invalid');
