CREATE OR REPLACE TEMP VIEW bm509_gap_range_window AS
SELECT
  grp,
  event_ts,
  amount,
  SUM(amount) OVER (
    PARTITION BY grp
    ORDER BY event_ts
    RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
  ) AS rolling_sum_range_1d
FROM gap_window_rows;
