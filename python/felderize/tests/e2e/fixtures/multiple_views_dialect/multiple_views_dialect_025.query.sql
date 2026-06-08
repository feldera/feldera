CREATE VIEW multiple_views_dialect_025_raw AS
SELECT mid, dim, delta FROM meters WHERE mid > 0;

CREATE VIEW multiple_views_dialect_025_calc AS
SELECT mid, dim, abs(delta) AS mag, pmod(delta, 5) AS bucket
FROM multiple_views_dialect_025_raw;

CREATE VIEW multiple_views_dialect_025_agg AS
SELECT bucket, SUM(mag) AS total_mag, COUNT(*) AS n
FROM multiple_views_dialect_025_calc GROUP BY bucket;

CREATE VIEW multiple_views_dialect_025_kept AS
SELECT bucket, total_mag, n FROM multiple_views_dialect_025_agg WHERE n >= 2;

CREATE VIEW multiple_views_dialect_025 AS
SELECT bucket, total_mag, n FROM multiple_views_dialect_025_kept;
