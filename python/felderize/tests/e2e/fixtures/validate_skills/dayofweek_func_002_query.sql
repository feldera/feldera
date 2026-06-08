-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW transaction_weekday_report AS SELECT trans_id, trans_date, amount, DAYOFWEEK(trans_date) AS weekday_num FROM transaction_records WHERE amount > 100;
