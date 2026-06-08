-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE TABLE transaction_records (tx_id INT, tx_timestamp TIMESTAMP, amount DECIMAL(10,2));
