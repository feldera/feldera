-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE TABLE event_log (event_id INT, year_val INT, month_val INT, day_val INT, hour_val INT, minute_val INT, second_val INT);
