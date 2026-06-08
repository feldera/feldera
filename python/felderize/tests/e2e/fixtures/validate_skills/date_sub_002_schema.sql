-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE TABLE transaction_dates (txn_id INT, txn_date DATE, subtract_days INT); CREATE TABLE transaction_expected (txn_id INT, adjusted_date DATE);
