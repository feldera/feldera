CREATE VIEW multiple_views_dialect_007_raw AS
SELECT pid, first, last, city FROM people WHERE pid > 0;

CREATE VIEW multiple_views_dialect_007_named AS
SELECT pid, concat_ws(' ', first, last) AS full_name, upper(city) AS city
FROM multiple_views_dialect_007_raw;

CREATE VIEW multiple_views_dialect_007_agg AS
SELECT city, COUNT(*) AS n, MIN(full_name) AS first_alpha
FROM multiple_views_dialect_007_named GROUP BY city;

CREATE VIEW multiple_views_dialect_007_kept AS
SELECT city, n FROM multiple_views_dialect_007_agg WHERE n >= 2;

CREATE VIEW multiple_views_dialect_007 AS
SELECT city, n FROM multiple_views_dialect_007_kept;
