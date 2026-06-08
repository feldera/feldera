-- rule: date_trunc_week
-- spark: date_trunc('WEEK', d) / trunc(d, 'WEEK') — truncate date to start of week; Spark uses Monday as first day of week
-- feldera: DATE_TRUNC(d - INTERVAL '1' DAY, WEEK) + INTERVAL '1' DAY — Feldera truncates to Sunday; subtract 1 day before truncating to handle all days correctly
CREATE OR REPLACE TEMP VIEW weekly_sales_v1 AS SELECT sale_date, date_trunc('WEEK', sale_date) AS week_start, amount FROM sales_log ORDER BY sale_date;
