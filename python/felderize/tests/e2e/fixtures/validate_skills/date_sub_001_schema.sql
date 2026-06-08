-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE TABLE event_log (event_id INT, event_date DATE, days_back INT); CREATE TABLE event_log_expected (event_id INT, result_date DATE);
