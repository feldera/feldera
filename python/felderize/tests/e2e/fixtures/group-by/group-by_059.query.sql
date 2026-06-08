CREATE VIEW group-by_059 AS
SELECT count(*)
FROM VALUES (Map(1, Array(Map(1,2)), 2, Array(Map(2, 3, 1, 2)))), (Map(2, Array(Map(1, 2, 2,3)), 1, Array(Map(1, 2)))), (Map(1, Array(Map(1,2)), 2, Array(Map(2, 4)))) as t(a)
GROUP BY a;
