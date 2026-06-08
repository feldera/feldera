-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE OR REPLACE TEMP VIEW event_dates_adjusted_v1 AS SELECT event_id, event_date, date_add(event_date, 7) AS week_later FROM event_dates;
