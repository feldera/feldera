-- rule: bround
-- spark: BROUND(x, d) — round using banker's rounding (half-to-even) to d decimal places; use DECIMAL input
-- feldera: BROUND(x, d) — same function, supported for DECIMAL input in Feldera; add note that FLOAT/DOUBLE input is not supported
CREATE OR REPLACE TEMP VIEW bround_results_v2 AS SELECT invoice_id, line_amount, BROUND(line_amount, 2) AS amount_rounded FROM invoice_detail;
