-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE OR REPLACE TEMP VIEW pmod_result_v1 AS SELECT id, amount, divisor, pmod(amount, divisor) AS positive_remainder FROM transactions;
