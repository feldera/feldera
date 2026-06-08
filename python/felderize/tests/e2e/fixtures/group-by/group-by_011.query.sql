CREATE VIEW group-by_011 AS
SELECT k, every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg GROUP BY k;
