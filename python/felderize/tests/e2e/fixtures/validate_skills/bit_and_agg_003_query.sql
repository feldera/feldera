-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE OR REPLACE TEMP VIEW user_permission_final AS SELECT user_id, bit_and(permission) AS common_access FROM permission_bits GROUP BY user_id;
