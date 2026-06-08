-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW effective_prices AS SELECT product_id, COALESCE(sale_price, discount_price, list_price, 0.0) AS final_price FROM price_data;
