CREATE VIEW group-by_058 AS
SELECT count(*)
FROM VALUES (Map(1, Map(1,2), 2, Map(2, 3, 1, 2))), (Map(2, Map(1, 2, 2,3), 1, Map(1, 2))), (Map(1, Map(1,2), 2, Map(2, 4))) as t(a)
GROUP BY a;
