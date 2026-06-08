-- rule: last_value_window
-- spark: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — last value in window partition
-- feldera: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) — must use explicit unbounded ROWS frame; default frame is not supported
CREATE OR REPLACE TEMP VIEW stock_latest_v3 AS SELECT ticker, price, LAST_VALUE(price) OVER (PARTITION BY ticker ORDER BY trade_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_price_per_ticker FROM stock_price_v3;
