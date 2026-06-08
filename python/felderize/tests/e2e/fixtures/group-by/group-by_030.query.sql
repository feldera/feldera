CREATE VIEW group-by_030 AS
SELECT histogram_numeric(col, 3) FROM VALUES (1D), (2D), (3D) AS tab(col);
