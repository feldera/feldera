-- rule: bit_or_agg
-- spark: bit_or(col) — bitwise OR aggregate over all rows in group
-- feldera: BIT_OR(col)
CREATE OR REPLACE TEMP VIEW permission_combined_v2 AS SELECT dept, BIT_OR(perm_mask) AS all_perms FROM permissions_v2 GROUP BY dept;
