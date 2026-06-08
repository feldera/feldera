-- rule: year_month_func
-- spark: YEAR(d) — extract year from DATE/TIMESTAMP; MONTH(d) — extract month (1-12)
-- feldera: YEAR(d) / MONTH(d) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW events_summary AS SELECT
  event_id,
  event_name,
  event_date,
  YEAR(event_date) AS year_extracted,
  MONTH(event_date) AS month_extracted
FROM events_log
WHERE YEAR(event_date) >= 2024;
