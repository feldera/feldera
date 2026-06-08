-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE TABLE order_details_v3 (country STRING, category STRING, revenue DECIMAL(10,2), order_count INT);
