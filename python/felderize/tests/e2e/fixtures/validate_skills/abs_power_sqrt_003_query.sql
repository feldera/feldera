-- rule: abs_power_sqrt
-- spark: ABS(x) — absolute value; POWER(x, p) — exponentiation; SQRT(x) — square root
-- feldera: ABS(x) / POWER(x, p) / SQRT(x) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW financial_calc_v3 AS SELECT transaction_id, ABS(amount) AS abs_amount, POWER(1 + interest_rate, 10) AS compound_factor, SQRT(ABS(amount)) AS amount_sqrt FROM financial_data WHERE amount <> 0;
