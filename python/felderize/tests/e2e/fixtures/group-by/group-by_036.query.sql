CREATE VIEW group-by_036 AS
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS DECIMAL(4, 2))), (CAST(2 AS DECIMAL(4, 2))), (CAST(3 AS DECIMAL(4, 2))) AS tab(col);
