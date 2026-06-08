CREATE VIEW group-by_009 AS
SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE k = 4;
