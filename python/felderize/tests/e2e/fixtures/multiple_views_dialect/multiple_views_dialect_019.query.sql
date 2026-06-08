CREATE VIEW multiple_views_dialect_019_raw AS
SELECT eid, code, val FROM events WHERE eid > 0;

CREATE VIEW multiple_views_dialect_019_bucketed AS
SELECT eid,
       decode(code, 1, 'low', 2, 'mid', 3, 'high', 'other') AS bucket,
       val
FROM multiple_views_dialect_019_raw;

CREATE VIEW multiple_views_dialect_019_agg AS
SELECT bucket, SUM(val) AS total, COUNT(*) AS n
FROM multiple_views_dialect_019_bucketed GROUP BY bucket;

CREATE VIEW multiple_views_dialect_019_kept AS
SELECT bucket, total, n FROM multiple_views_dialect_019_agg WHERE total >= 5;

CREATE VIEW multiple_views_dialect_019 AS
SELECT bucket, total, n FROM multiple_views_dialect_019_kept;
