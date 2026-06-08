-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_006_clean AS
SELECT sale_id AS id, product AS dim, units AS val
FROM sales
WHERE units > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_006_filtered AS
SELECT id, dim, val FROM multiple_views_006_clean WHERE val >= 1;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_006_agg AS
SELECT dim, SUM(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_006_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_006_enriched AS
SELECT a.dim AS dim, d.category AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_006_agg a JOIN products d ON a.dim = d.product;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_006 AS
SELECT label, agg_val, n
FROM multiple_views_006_enriched
WHERE n >= 1;
