CREATE OR REPLACE TEMP VIEW fe006_edge_unpivot AS
SELECT *
FROM feldera_edge_wide_metrics
UNPIVOT (
  amount FOR quarter IN (q1_amt, q2_amt, q3_amt)
);
