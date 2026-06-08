-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE TABLE sales_data_v1 (region STRING, product STRING, amount DECIMAL(10,2), quarter INT);
