-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_017_clean AS
SELECT enroll_id AS id, course AS dim, score AS val
FROM enrollments
WHERE score > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_017_filtered AS
SELECT id, dim, val FROM multiple_views_017_clean WHERE val >= 8;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_017_agg AS
SELECT dim, MAX(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_017_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_017_enriched AS
SELECT a.dim AS dim, d.faculty AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_017_agg a JOIN courses d ON a.dim = d.course;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_017 AS
SELECT label, agg_val, n
FROM multiple_views_017_enriched
WHERE n >= 1;
