-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE OR REPLACE TEMP VIEW sorted_metrics_v3 AS SELECT sensor_id, location, sort_array(readings, true) AS readings_asc, sort_array(readings, false) AS readings_desc FROM decimal_metrics;
