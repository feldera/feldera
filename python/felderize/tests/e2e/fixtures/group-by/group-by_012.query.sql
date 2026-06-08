CREATE VIEW group-by_012 AS
SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) = false;
