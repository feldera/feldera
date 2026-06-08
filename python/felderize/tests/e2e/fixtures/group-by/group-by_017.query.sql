CREATE VIEW group-by_017 AS
SELECT k, v, every(v) OVER (PARTITION BY k ORDER BY v) FROM test_agg;
