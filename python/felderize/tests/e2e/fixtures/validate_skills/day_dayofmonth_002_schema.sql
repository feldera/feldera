-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE TABLE transaction_records (txn_id INT, txn_timestamp TIMESTAMP, amount DECIMAL(10,2));
