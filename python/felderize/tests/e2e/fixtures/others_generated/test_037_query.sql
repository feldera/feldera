CREATE OR REPLACE TEMP VIEW bm52_calendar_parts AS
SELECT event_id, YEAR(event_time) AS event_year, MONTH(event_time) AS event_month, DAY(event_time) AS event_day FROM calendar_events;
