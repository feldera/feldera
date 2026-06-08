-- rule: to_date_func
-- spark: to_date(ts) / to_date(ts, fmt) — convert to DATE
-- feldera: CAST(ts AS DATE) — format parameter is ignored
CREATE OR REPLACE TEMP VIEW transaction_dates_v2 AS SELECT tx_id, to_date(tx_timestamp, 'yyyy-MM-dd HH:mm:ss') AS tx_date, amount FROM transaction_records;
