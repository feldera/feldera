-- rule: avg_integer
-- spark: AVG(int_col) — Spark returns DOUBLE (e.g. AVG(1,2) = 1.5); Feldera returns INT (= 1)
-- feldera: AVG(CAST(int_col AS DOUBLE)) — cast integer input to DOUBLE to match Spark's return type
CREATE TABLE daily_clicks (
  day_id INT,
  page_id INT,
  clicks INT,
  impressions INT
);
