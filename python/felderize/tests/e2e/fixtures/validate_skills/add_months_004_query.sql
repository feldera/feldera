-- rule: add_months
-- spark: add_months(date, n) — unquoted literal form
-- feldera: date + INTERVAL n MONTH (unquoted integer literal, equivalent to INTERVAL 'n' MONTH)
CREATE VIEW add_months_004_v AS
SELECT id,
       event_date + INTERVAL 3 MONTH AS plus_3_months,
       event_date + INTERVAL 12 MONTH AS plus_12_months
FROM events;
