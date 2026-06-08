CREATE OR REPLACE TEMP VIEW bm543_creative_bool_agg AS
SELECT
  region,
  bool_or(amount > 100) AS any_big_sale,
  bool_and(units > 0) AS all_positive_units
FROM creative_sales
GROUP BY region;
