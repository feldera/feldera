-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_003_clean AS
SELECT order_id AS id, customer AS dim, amount AS val
FROM orders
WHERE amount > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_003_filtered AS
SELECT id, dim, val FROM multiple_views_003_clean WHERE val >= 12;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_003_agg AS
SELECT dim, MIN(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_003_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_003_enriched AS
SELECT a.dim AS dim, d.region AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_003_agg a JOIN customers d ON a.dim = d.customer;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_003 AS
SELECT label, agg_val, n
FROM multiple_views_003_enriched
WHERE n >= 2;
