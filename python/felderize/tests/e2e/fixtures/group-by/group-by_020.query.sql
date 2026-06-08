CREATE VIEW group-by_020 AS
SELECT k, v, bool_and(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
