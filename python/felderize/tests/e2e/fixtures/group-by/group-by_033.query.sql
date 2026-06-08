CREATE VIEW group-by_033 AS
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS TINYINT)), (CAST(2 AS TINYINT)), (CAST(3 AS TINYINT)) AS tab(col);
