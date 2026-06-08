CREATE VIEW group-by_019 AS
SELECT k, v, any(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
