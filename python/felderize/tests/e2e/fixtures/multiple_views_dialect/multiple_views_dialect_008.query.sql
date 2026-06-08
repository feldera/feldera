CREATE VIEW multiple_views_dialect_008_raw AS
SELECT pid, first, last, city FROM people WHERE pid > 0;

CREATE VIEW multiple_views_dialect_008_named AS
SELECT pid, concat_ws(' ', first, last) AS full_name, upper(city) AS city
FROM multiple_views_dialect_008_raw;

CREATE VIEW multiple_views_dialect_008_agg AS
SELECT city, COUNT(*) AS n, MIN(full_name) AS first_alpha
FROM multiple_views_dialect_008_named GROUP BY city;

CREATE VIEW multiple_views_dialect_008_kept AS
SELECT city, n FROM multiple_views_dialect_008_agg WHERE n >= 3;

CREATE VIEW multiple_views_dialect_008 AS
SELECT city, n FROM multiple_views_dialect_008_kept;
