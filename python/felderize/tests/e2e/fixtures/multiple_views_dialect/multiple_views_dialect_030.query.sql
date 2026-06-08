CREATE VIEW multiple_views_dialect_030_raw AS
SELECT eid, email FROM emails WHERE eid > 0;

CREATE VIEW multiple_views_dialect_030_parts AS
SELECT eid,
       left(email, instr(email, '@') - 1) AS username,
       substr(email, instr(email, '@') + 1) AS domain
FROM multiple_views_dialect_030_raw;

CREATE VIEW multiple_views_dialect_030_agg AS
SELECT domain, COUNT(*) AS n, MIN(username) AS first_user
FROM multiple_views_dialect_030_parts GROUP BY domain;

CREATE VIEW multiple_views_dialect_030_kept AS
SELECT domain, n FROM multiple_views_dialect_030_agg WHERE n >= 2;

CREATE VIEW multiple_views_dialect_030 AS
SELECT domain, n FROM multiple_views_dialect_030_kept;
