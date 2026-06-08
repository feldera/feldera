-- rule: natural_join
-- spark: NATURAL JOIN — auto-join on all matching column names
-- feldera: NATURAL JOIN — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW natural_join_v3 AS SELECT * FROM products NATURAL JOIN categories;
