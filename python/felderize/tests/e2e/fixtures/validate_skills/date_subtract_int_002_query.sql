-- rule: date_subtract_int
-- spark: date_col - integer_expr — subtract an integer number of days from a DATE column
-- feldera: date_col - integer_expr * INTERVAL '1' DAY — Feldera does not support DATE minus integer directly; multiply by INTERVAL '1' DAY
CREATE OR REPLACE TEMP VIEW refund_deadline_v2 AS SELECT tx_id, purchase_date - refund_window AS deadline FROM transaction_dates WHERE refund_window > 0;
