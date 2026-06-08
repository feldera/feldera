-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE TABLE sales_records (
  sale_id INT,
  transaction_date DATE,
  amount DOUBLE
);
