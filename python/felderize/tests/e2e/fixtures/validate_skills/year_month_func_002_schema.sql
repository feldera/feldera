-- rule: year_month_func
-- spark: YEAR(d) — extract year from DATE/TIMESTAMP; MONTH(d) — extract month (1-12)
-- feldera: YEAR(d) / MONTH(d) — both work identically in Feldera, no translation needed
CREATE TABLE transaction_history (
  trans_id BIGINT,
  trans_timestamp TIMESTAMP,
  amount DOUBLE
);
