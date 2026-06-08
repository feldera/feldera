CREATE VIEW group-by_042 AS
SELECT histogram_numeric(col, 3)
FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col);
