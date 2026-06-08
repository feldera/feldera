-- rule: date_sub
-- spark: date_sub(date, n) — subtract n days from a date
-- feldera: date - INTERVAL 'n' DAY  (literal n) or  date - n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW event_results_v1 AS SELECT event_id, date_sub(event_date, 7) AS result_date FROM event_log;
