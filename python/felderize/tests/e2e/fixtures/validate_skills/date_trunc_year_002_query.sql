-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE OR REPLACE TEMP VIEW annual_transactions_v2 AS SELECT txn_id, amount, date_trunc('YEAR', txn_timestamp) AS fiscal_year_start FROM transaction_records;
