-- rule: year_month_func
-- spark: YEAR(d) — extract year from DATE/TIMESTAMP; MONTH(d) — extract month (1-12)
-- feldera: YEAR(d) / MONTH(d) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW orders_by_year_month AS SELECT
  order_id,
  customer_id,
  order_date,
  YEAR(order_date) AS order_year,
  MONTH(order_date) AS order_month,
  CASE
    WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
    WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
    WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
    ELSE 'Fall'
  END AS season,
  status
FROM order_records
WHERE MONTH(order_date) >= 3 AND MONTH(order_date) <= 11;
