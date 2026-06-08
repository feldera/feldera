-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE TABLE sales_data_1 (region STRING, product STRING, year INT, revenue DECIMAL(10,2));
