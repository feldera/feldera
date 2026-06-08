CREATE VIEW group-by_035 AS
SELECT histogram_numeric(col, 3) FROM VALUES
  (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)) AS tab(col);
