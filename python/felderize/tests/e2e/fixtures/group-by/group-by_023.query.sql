CREATE VIEW group-by_023 AS
SELECT k, max(v) FROM test_agg GROUP BY k HAVING max(v) = true;
