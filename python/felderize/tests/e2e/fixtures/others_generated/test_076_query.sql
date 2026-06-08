CREATE OR REPLACE TEMP VIEW val56_median_amount AS
SELECT grp, percentile_approx(amount, 0.5) AS median_amount
FROM metric_observations
GROUP BY grp;
