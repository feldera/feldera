-- rule: day_dayofmonth
-- spark: DAY(d) / day(d) — extract day-of-month from date or timestamp
-- feldera: DAYOFMONTH(d)
CREATE OR REPLACE TEMP VIEW day_extraction_v2 AS SELECT txn_id, txn_timestamp, DAY(txn_timestamp) AS day_of_month, amount FROM transaction_records ORDER BY day_of_month;
