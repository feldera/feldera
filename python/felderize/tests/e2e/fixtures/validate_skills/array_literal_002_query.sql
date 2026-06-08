-- rule: array_literal
-- spark: array(v1, v2, ...) — construct an array literal from values
-- feldera: ARRAY(v1, v2, ...) — same syntax in Feldera; also ARRAY[v1, v2, ...] works
CREATE OR REPLACE TEMP VIEW customer_bonus_array AS SELECT customer_id, array(score, 100, 200, 300) AS bonus_tiers FROM customer_scores WHERE score > 0;
