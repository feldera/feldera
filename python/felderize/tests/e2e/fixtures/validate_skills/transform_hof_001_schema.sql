-- rule: transform_hof
-- spark: transform(arr, x -> expr) — apply lambda to each array element, return transformed array
-- feldera: TRANSFORM(arr, x -> expr) — same syntax, supported directly in Feldera
CREATE TABLE product_prices (product_id INT, prices ARRAY<DECIMAL(10,2)>);
