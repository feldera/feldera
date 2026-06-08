CREATE OR REPLACE TEMP VIEW val52_paid_amounts AS
SELECT grp, SUM(amount) FILTER (WHERE status = 'PAID') AS paid_amount
FROM metric_observations
GROUP BY grp;
