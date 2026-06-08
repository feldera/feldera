-- rule: mod_func
-- spark: MOD(a, b) / a % b — modulo operator; sign follows dividend (truncation toward zero)
-- feldera: MOD(a, b) / a % b — same, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW sales_remainder_v1 AS SELECT transaction_id, amount, divisor, MOD(amount, divisor) AS remainder, amount % divisor AS alt_remainder FROM sales_data;
