-- rule: trunc_date
-- spark: trunc(d, 'YYYY'/'MM'/'QUARTER'/'WEEK') — truncate date using string unit; ONLY these formats work in Spark 4.x trunc() — do NOT use 'Q', 'DD', or 'DAY' as they return NULL
-- feldera: DATE_TRUNC(d, YEAR/MONTH/QUARTER/WEEK) — string unit becomes SQL keyword
CREATE OR REPLACE TEMP VIEW monthly_invoices_v2 AS
SELECT
  trunc(invoice_date, 'MM') AS month_start,
  COUNT(*) AS invoice_count,
  SUM(invoice_amount) AS monthly_total
FROM invoice_records
GROUP BY trunc(invoice_date, 'MM')
ORDER BY month_start;
