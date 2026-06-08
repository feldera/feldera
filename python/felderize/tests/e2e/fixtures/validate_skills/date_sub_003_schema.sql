-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE TABLE schedule_dates (schedule_id INT, base_date DATE, lookback_days INT); CREATE TABLE schedule_expected (schedule_id INT, prior_date DATE);
