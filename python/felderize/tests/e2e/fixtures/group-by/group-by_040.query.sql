CREATE VIEW group-by_040 AS
SELECT histogram_numeric(col, 3)
FROM VALUES (NULL), (NULL), (NULL) AS tab(col);
