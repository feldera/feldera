-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE TABLE transaction_records (trans_id INT, yr INT, mn INT, dy INT, hr INT, min INT, sec INT, amount DECIMAL(10, 2));
