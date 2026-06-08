-- rule: hour_minute_second_func
-- spark: HOUR(ts) — extract hour (0-23); MINUTE(ts) — extract minute (0-59); SECOND(ts) — extract second (0-59)
-- feldera: HOUR(ts) / MINUTE(ts) / SECOND(ts) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW txn_time_parts_v2 AS SELECT txn_id, timestamp_val, HOUR(timestamp_val) AS txn_hour, MINUTE(timestamp_val) AS txn_minute, SECOND(timestamp_val) AS txn_second, amount FROM transaction_ts WHERE HOUR(timestamp_val) >= 9 AND HOUR(timestamp_val) <= 17;
