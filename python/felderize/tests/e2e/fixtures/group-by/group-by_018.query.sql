CREATE VIEW group-by_018 AS
SELECT k, v, some(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
