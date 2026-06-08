-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW scheduled_events_v2 AS SELECT event_id, event_name, MAKE_DATE(event_year, event_month, event_day) AS event_date FROM event_scheduling WHERE MAKE_DATE(event_year, event_month, event_day) >= CAST('2020-01-01' AS DATE) ORDER BY event_id;
