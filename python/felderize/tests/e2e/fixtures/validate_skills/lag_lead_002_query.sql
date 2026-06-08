-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE OR REPLACE TEMP VIEW price_trends_v2 AS SELECT ticker, trade_date, closing_price, volume, LAG(closing_price, 2, NULL) OVER (PARTITION BY ticker ORDER BY trade_date) AS price_2_days_ago, LEAD(closing_price, 2, NULL) OVER (PARTITION BY ticker ORDER BY trade_date) AS price_in_2_days FROM stock_prices;
