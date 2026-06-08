CREATE VIEW group-by_022 AS
SELECT count(*) FROM test_agg HAVING count(*) > 1L;
