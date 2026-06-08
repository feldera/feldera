-- rule: avg_integer
-- spark: AVG(int_col) — Spark returns DOUBLE (e.g. AVG(1,2) = 1.5); Feldera returns INT (= 1)
-- feldera: AVG(CAST(int_col AS DOUBLE)) — cast integer input to DOUBLE to match Spark's return type
CREATE OR REPLACE TEMP VIEW page_avg_metrics AS SELECT page_id, AVG(clicks) as avg_clicks, AVG(impressions) as avg_impressions FROM daily_clicks GROUP BY page_id;
