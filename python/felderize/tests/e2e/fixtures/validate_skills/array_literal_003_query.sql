-- rule: array_literal
-- spark: array(v1, v2, ...) — construct an array literal from values
-- feldera: ARRAY(v1, v2, ...) — same syntax in Feldera; also ARRAY[v1, v2, ...] works
CREATE OR REPLACE TEMP VIEW region_thresholds AS SELECT region_id, region_name, array('small', 'medium', 'large', 'metropolis') AS size_categories, array(population, 100000, 500000, 1000000) AS metrics FROM region_data;
