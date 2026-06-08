-- rule: safe_cast_default
-- spark: TRY_CAST(invalid_str AS DOUBLE) / TRY_CAST(invalid_str AS BOOLEAN) — Spark returns NULL on failure; Feldera SAFE_CAST returns type default (0.0 for DOUBLE, false for BOOLEAN) instead of NULL
-- feldera: UNSUPPORTED — SAFE_CAST on REAL/DOUBLE/BOOLEAN targets does not return NULL on failure in Feldera; it returns the type default instead. No exact NULL-on-failure equivalent exists. Mark unsupported when NULL-on-failure semantics matter.
CREATE TABLE parse_strings_1 (id INT, value_str STRING);
