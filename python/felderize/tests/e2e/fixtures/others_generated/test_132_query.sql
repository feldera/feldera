CREATE OR REPLACE TEMP VIEW bm544_creative_stack AS
SELECT id, quarter, amount
FROM creative_wide
LATERAL VIEW stack(3, 'Q1', q1_amt, 'Q2', q2_amt, 'Q3', q3_amt) s AS quarter, amount;
