-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE TABLE decimal_metrics (sensor_id INT, location STRING, readings ARRAY<DECIMAL(10,2)>);
