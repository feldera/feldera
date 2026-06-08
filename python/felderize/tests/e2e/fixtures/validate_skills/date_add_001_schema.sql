-- rule: date_add
-- spark: date_add(date, n) — add n days to a date
-- feldera: date + INTERVAL 'n' DAY  (literal n) or  date + n * INTERVAL '1' DAY  (column n)
CREATE TABLE event_dates (event_id INT, event_date DATE); CREATE TABLE date_offsets (offset_id INT, days_to_add INT);
