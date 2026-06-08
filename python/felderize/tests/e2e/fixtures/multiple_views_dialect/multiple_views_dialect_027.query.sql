CREATE VIEW multiple_views_dialect_027_raw AS
SELECT eid, email FROM emails WHERE eid > 0;

CREATE VIEW multiple_views_dialect_027_parts AS
SELECT eid,
       left(email, instr(email, '@') - 1) AS username,
       substr(email, instr(email, '@') + 1) AS domain
FROM multiple_views_dialect_027_raw;

CREATE VIEW multiple_views_dialect_027_agg AS
SELECT domain, COUNT(*) AS n, MIN(username) AS first_user
FROM multiple_views_dialect_027_parts GROUP BY domain;

CREATE VIEW multiple_views_dialect_027_kept AS
SELECT domain, n FROM multiple_views_dialect_027_agg WHERE n >= 2;

CREATE VIEW multiple_views_dialect_027 AS
SELECT domain, n FROM multiple_views_dialect_027_kept;
