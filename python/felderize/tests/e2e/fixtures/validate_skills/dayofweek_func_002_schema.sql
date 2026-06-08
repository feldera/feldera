-- rule: dayofweek_func
-- spark: DAYOFWEEK(d) — day of week as integer (1=Sunday, 2=Monday, ..., 7=Saturday)
-- feldera: DAYOFWEEK(d) — same function, supported directly in Feldera
CREATE TABLE transaction_records (trans_id INT, trans_date DATE, amount DECIMAL(10, 2));
