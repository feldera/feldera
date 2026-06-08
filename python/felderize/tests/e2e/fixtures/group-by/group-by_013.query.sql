CREATE VIEW group-by_013 AS
SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) IS NULL;
