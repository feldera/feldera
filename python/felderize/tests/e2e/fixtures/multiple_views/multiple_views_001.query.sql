-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_001_clean AS
SELECT order_id AS id, customer AS dim, amount AS val
FROM orders
WHERE amount > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_001_filtered AS
SELECT id, dim, val FROM multiple_views_001_clean WHERE val >= 1;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_001_agg AS
SELECT dim, SUM(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_001_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_001_enriched AS
SELECT a.dim AS dim, d.region AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_001_agg a JOIN customers d ON a.dim = d.customer;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_001 AS
SELECT label, agg_val, n
FROM multiple_views_001_enriched
WHERE n >= 1;
