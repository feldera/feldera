-- rule: date_format_dayofweek
-- spark: date_format(d, 'E') — extract abbreviated day-of-week name from a DATE (e.g. 'Mon', 'Tue', 'Wed')
-- feldera: FORMAT_DATE('%a', d) — Rust strftime %a gives the same 3-letter English abbreviation as Spark's 'E' format
CREATE OR REPLACE TEMP VIEW sales_view AS SELECT sale_id, transaction_date, date_format(transaction_date, 'E') AS day_of_week, amount FROM sales_records ORDER BY transaction_date;
