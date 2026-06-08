CREATE VIEW group-by_008 AS
SELECT every(v), some(v), any(v), bool_and(v), bool_or(v) FROM test_agg WHERE 1 = 0;
