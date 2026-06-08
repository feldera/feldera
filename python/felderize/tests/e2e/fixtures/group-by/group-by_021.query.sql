CREATE VIEW group-by_021 AS
SELECT k, v, bool_or(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
