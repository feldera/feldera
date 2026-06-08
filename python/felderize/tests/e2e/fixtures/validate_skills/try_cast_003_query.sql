-- rule: try_cast
-- spark: TRY_CAST(expr AS type) — cast that returns NULL on failure instead of raising an error
-- feldera: SAFE_CAST(expr AS type) — Feldera's exact equivalent; returns NULL on failure
CREATE OR REPLACE TEMP VIEW product_casted AS SELECT product_id, TRY_CAST(stock_str AS INT) AS inventory, TRY_CAST(discount_str AS DOUBLE) AS discount_rate, TRY_CAST(active_str AS BOOLEAN) AS is_active FROM product_info;
