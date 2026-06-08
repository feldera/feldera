CREATE VIEW group-by_043 AS
SELECT
  collect_list(col),
  array_agg(col)
FROM VALUES
  (1), (2), (1) AS tab(col);
