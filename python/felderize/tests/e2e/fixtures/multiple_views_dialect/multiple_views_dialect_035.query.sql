CREATE VIEW multiple_views_dialect_035_raw AS
SELECT sid, team, a, b FROM scores WHERE sid > 0;

CREATE VIEW multiple_views_dialect_035_eval AS
SELECT sid, team,
       greatest(a, b) AS hi,
       least(a, b) AS lo,
       CASE WHEN a > b THEN 'A' ELSE 'B' END AS winner
FROM multiple_views_dialect_035_raw;

CREATE VIEW multiple_views_dialect_035_agg AS
SELECT winner, SUM(hi) AS sum_hi, SUM(lo) AS sum_lo, COUNT(*) AS n
FROM multiple_views_dialect_035_eval GROUP BY winner;

CREATE VIEW multiple_views_dialect_035_kept AS
SELECT winner, sum_hi, sum_lo, n FROM multiple_views_dialect_035_agg WHERE n >= 2;

CREATE VIEW multiple_views_dialect_035 AS
SELECT winner, sum_hi, sum_lo, n FROM multiple_views_dialect_035_kept;
