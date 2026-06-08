-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE TABLE transactions (id INT, amount INT, divisor INT);
