-- rule: ceil_floor_type
-- spark: CEIL(x) / FLOOR(x) on DOUBLE — Spark returns BIGINT; Feldera returns DOUBLE
-- feldera: CEIL(x) / FLOOR(x) — same function, add warning about return type difference
CREATE TABLE financial_amounts (transaction_id INT, amount DOUBLE);
