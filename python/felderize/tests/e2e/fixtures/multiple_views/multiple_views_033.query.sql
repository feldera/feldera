-- Stage 1: base rows above a threshold.
CREATE VIEW multiple_views_033_base AS
SELECT visit_id AS id, page AS dim, duration AS val
FROM visits
WHERE duration >= 12;

-- Stage 2: per-dimension maxima (depends on _base).
CREATE VIEW multiple_views_033_maxes AS
SELECT dim, MAX(val) AS mx FROM multiple_views_033_base GROUP BY dim;

-- Stage 3: tag each base row with its dimension max
--          (depends on BOTH _base and _maxes).
CREATE VIEW multiple_views_033_marked AS
SELECT b.id AS id, b.dim AS dim, b.val AS val, m.mx AS mx
FROM multiple_views_033_base b JOIN multiple_views_033_maxes m ON b.dim = m.dim;

-- Stage 4: roll up per dimension (depends on _marked).
CREATE VIEW multiple_views_033_rollup AS
SELECT dim, SUM(val) AS total, MAX(mx) AS top
FROM multiple_views_033_marked GROUP BY dim;

-- Stage 5: final output (depends on _rollup).
CREATE VIEW multiple_views_033 AS
SELECT dim, total, top
FROM multiple_views_033_rollup
WHERE total >= 15;
