-- rule: last_value_window
-- spark: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — last value in window partition
-- feldera: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) — must use explicit unbounded ROWS frame; default frame is not supported
CREATE TABLE stock_price_v3 (ticker STRING, price DOUBLE, trade_timestamp TIMESTAMP);
