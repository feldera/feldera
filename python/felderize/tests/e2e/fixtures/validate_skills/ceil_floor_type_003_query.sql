-- rule: ceil_floor_type
-- spark: CEIL(x) / FLOOR(x) on DOUBLE — Spark returns BIGINT; Feldera returns DOUBLE
-- feldera: CEIL(x) / FLOOR(x) — same function, add warning about return type difference
CREATE OR REPLACE TEMP VIEW financial_rounded AS SELECT transaction_id, CEIL(amount) AS ceil_amount, FLOOR(amount) AS floor_amount FROM financial_amounts WHERE amount IS NOT NULL;
