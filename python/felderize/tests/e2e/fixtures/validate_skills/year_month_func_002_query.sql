-- rule: year_month_func
-- spark: YEAR(d) — extract year from DATE/TIMESTAMP; MONTH(d) — extract month (1-12)
-- feldera: YEAR(d) / MONTH(d) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW monthly_transactions AS SELECT
  YEAR(trans_timestamp) AS trans_year,
  MONTH(trans_timestamp) AS trans_month,
  COUNT(*) AS transaction_count,
  SUM(amount) AS total_amount
FROM transaction_history
GROUP BY YEAR(trans_timestamp), MONTH(trans_timestamp)
ORDER BY trans_year, trans_month;
