-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE OR REPLACE TEMP VIEW sales_nullif_v1 AS SELECT transaction_id, NULLIFZERO(amount) AS amount_or_null, NULLIFZERO(quantity) AS qty_or_null FROM sales_metrics;
