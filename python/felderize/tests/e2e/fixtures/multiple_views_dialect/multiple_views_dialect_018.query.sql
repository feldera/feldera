CREATE VIEW multiple_views_dialect_018_raw AS
SELECT eid, code, val FROM events WHERE eid > 0;

CREATE VIEW multiple_views_dialect_018_bucketed AS
SELECT eid,
       decode(code, 1, 'low', 2, 'mid', 3, 'high', 'other') AS bucket,
       val
FROM multiple_views_dialect_018_raw;

CREATE VIEW multiple_views_dialect_018_agg AS
SELECT bucket, SUM(val) AS total, COUNT(*) AS n
FROM multiple_views_dialect_018_bucketed GROUP BY bucket;

CREATE VIEW multiple_views_dialect_018_kept AS
SELECT bucket, total, n FROM multiple_views_dialect_018_agg WHERE total >= 25;

CREATE VIEW multiple_views_dialect_018 AS
SELECT bucket, total, n FROM multiple_views_dialect_018_kept;
