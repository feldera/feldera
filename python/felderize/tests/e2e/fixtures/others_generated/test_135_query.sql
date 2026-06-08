CREATE OR REPLACE TEMP VIEW fe012_edge_calendar_parts AS
SELECT
  row_id,
  d,
  dayofweek(d) AS dow,
  quarter(d) AS q,
  dayofmonth(d) AS dom,
  weekofyear(d) AS woy
FROM feldera_edge_calendar;
