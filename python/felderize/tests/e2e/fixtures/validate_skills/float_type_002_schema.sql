-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE TABLE product_metrics (product_id INT, price_usd FLOAT, rating FLOAT, stock_quantity INT);
