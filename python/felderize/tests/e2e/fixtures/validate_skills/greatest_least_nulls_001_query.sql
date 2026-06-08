-- rule: greatest_least_nulls
-- spark: greatest(a, b, ...) / least(a, b, ...) — return maximum/minimum value; Spark skips NULL values
-- feldera: GREATEST_IGNORE_NULLS(a, b, ...) / LEAST_IGNORE_NULLS(a, b, ...) — use the IGNORE_NULLS variant to match Spark's null-skipping semantics
CREATE OR REPLACE TEMP VIEW price_analysis_v1 AS SELECT id, greatest(price1, price2, price3) AS max_price, least(price1, price2, price3) AS min_price FROM price_points;
