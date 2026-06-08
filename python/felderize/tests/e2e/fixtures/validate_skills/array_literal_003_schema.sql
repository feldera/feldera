-- rule: array_literal
-- spark: array(v1, v2, ...) — construct an array literal from values
-- feldera: ARRAY(v1, v2, ...) — same syntax in Feldera; also ARRAY[v1, v2, ...] works
CREATE TABLE region_data (region_id INT, region_name STRING, population BIGINT);
