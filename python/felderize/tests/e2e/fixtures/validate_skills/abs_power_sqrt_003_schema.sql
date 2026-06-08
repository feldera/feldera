-- rule: abs_power_sqrt
-- spark: ABS(x) — absolute value; POWER(x, p) — exponentiation; SQRT(x) — square root
-- feldera: ABS(x) / POWER(x, p) / SQRT(x) — all work identically in Feldera, no translation needed
CREATE TABLE financial_data (transaction_id INT, amount DOUBLE, interest_rate DOUBLE);
