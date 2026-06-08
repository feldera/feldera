-- rule: transform_hof
-- spark: transform(arr, x -> expr) — apply lambda to each array element, return transformed array
-- feldera: TRANSFORM(arr, x -> expr) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW discounted_prices_v1 AS SELECT product_id, transform(prices, x -> x * CAST(0.9 AS DECIMAL(10,2))) AS discounted FROM product_prices;
