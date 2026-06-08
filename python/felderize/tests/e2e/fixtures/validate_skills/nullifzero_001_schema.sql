-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE TABLE sales_metrics (transaction_id INT, amount DECIMAL(10,2), quantity INT);
