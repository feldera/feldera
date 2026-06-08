-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE TABLE stock_prices (ticker STRING, trade_date DATE, closing_price DECIMAL(8, 2), volume INT);
