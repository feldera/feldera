CREATE VIEW multiple_views_dialect_034_raw AS
SELECT sid, team, a, b FROM scores WHERE sid > 0;

CREATE VIEW multiple_views_dialect_034_eval AS
SELECT sid, team,
       greatest(a, b) AS hi,
       least(a, b) AS lo,
       CASE WHEN a > b THEN 'A' ELSE 'B' END AS winner
FROM multiple_views_dialect_034_raw;

CREATE VIEW multiple_views_dialect_034_agg AS
SELECT winner, SUM(hi) AS sum_hi, SUM(lo) AS sum_lo, COUNT(*) AS n
FROM multiple_views_dialect_034_eval GROUP BY winner;

CREATE VIEW multiple_views_dialect_034_kept AS
SELECT winner, sum_hi, sum_lo, n FROM multiple_views_dialect_034_agg WHERE n >= 1;

CREATE VIEW multiple_views_dialect_034 AS
SELECT winner, sum_hi, sum_lo, n FROM multiple_views_dialect_034_kept;
