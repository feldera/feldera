CREATE VIEW multiple_views_dialect_006_raw AS
SELECT pid, first, last, city FROM people WHERE pid > 0;

CREATE VIEW multiple_views_dialect_006_named AS
SELECT pid, concat_ws(' ', first, last) AS full_name, upper(city) AS city
FROM multiple_views_dialect_006_raw;

CREATE VIEW multiple_views_dialect_006_agg AS
SELECT city, COUNT(*) AS n, MIN(full_name) AS first_alpha
FROM multiple_views_dialect_006_named GROUP BY city;

CREATE VIEW multiple_views_dialect_006_kept AS
SELECT city, n FROM multiple_views_dialect_006_agg WHERE n >= 1;

CREATE VIEW multiple_views_dialect_006 AS
SELECT city, n FROM multiple_views_dialect_006_kept;
