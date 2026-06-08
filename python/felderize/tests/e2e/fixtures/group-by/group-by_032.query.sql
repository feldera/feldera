CREATE VIEW group-by_032 AS
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS BYTE)), (CAST(2 AS BYTE)), (CAST(3 AS BYTE)) AS tab(col);
