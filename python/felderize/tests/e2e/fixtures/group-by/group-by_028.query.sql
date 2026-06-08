CREATE VIEW group-by_028 AS
SELECT histogram_numeric(col, 3) FROM VALUES (1L), (2L), (3L) AS tab(col);
