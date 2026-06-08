-- rule: sign_func
-- spark: sign(x) — returns -1.0, 0.0, or 1.0 depending on the sign of x (DOUBLE input/output)
-- feldera: SIGN(x) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW margin_classification_v3 AS SELECT transaction_id, category, margin_value, sign(margin_value) AS profitability FROM profit_margins;
