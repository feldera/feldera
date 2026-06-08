-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW transaction_times_v3 AS SELECT trans_id, amount, make_timestamp(yr, mn, dy, hr, min, sec) AS transaction_timestamp FROM transaction_records WHERE amount > 100;
