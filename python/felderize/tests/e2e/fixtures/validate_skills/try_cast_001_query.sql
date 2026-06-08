-- rule: try_cast
-- spark: TRY_CAST(expr AS type) — cast that returns NULL on failure instead of raising an error
-- feldera: SAFE_CAST(expr AS type) — Feldera's exact equivalent; returns NULL on failure
CREATE OR REPLACE TEMP VIEW metric_conversions AS SELECT id, TRY_CAST(score_str AS INT) AS score_int, TRY_CAST(price_str AS DECIMAL(10, 2)) AS price_decimal FROM user_metrics;
