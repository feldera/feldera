-- rule: last_day
-- spark: last_day(d) — last day of the month
-- feldera: DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY
CREATE TABLE transaction_dates (
  transaction_id INT,
  trans_date DATE,
  amount DECIMAL(10,2)
);
