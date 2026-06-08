CREATE VIEW multiple_views_dialect_024_raw AS
SELECT mid, dim, delta FROM meters WHERE mid > 0;

CREATE VIEW multiple_views_dialect_024_calc AS
SELECT mid, dim, abs(delta) AS mag, pmod(delta, 5) AS bucket
FROM multiple_views_dialect_024_raw;

CREATE VIEW multiple_views_dialect_024_agg AS
SELECT bucket, SUM(mag) AS total_mag, COUNT(*) AS n
FROM multiple_views_dialect_024_calc GROUP BY bucket;

CREATE VIEW multiple_views_dialect_024_kept AS
SELECT bucket, total_mag, n FROM multiple_views_dialect_024_agg WHERE n >= 1;

CREATE VIEW multiple_views_dialect_024 AS
SELECT bucket, total_mag, n FROM multiple_views_dialect_024_kept;
