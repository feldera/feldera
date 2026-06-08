-- rule: year_month_func
-- spark: YEAR(d) — extract year from DATE/TIMESTAMP; MONTH(d) — extract month (1-12)
-- feldera: YEAR(d) / MONTH(d) — both work identically in Feldera, no translation needed
CREATE TABLE order_records (
  order_id INT,
  customer_id INT,
  order_date DATE,
  status STRING
);
