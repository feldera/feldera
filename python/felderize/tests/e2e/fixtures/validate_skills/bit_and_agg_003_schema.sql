-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE TABLE permission_bits (user_id INT, permission INT);
CREATE TABLE audit_log (user_id INT, action STRING);
