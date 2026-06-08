CREATE VIEW group-by_049 AS
SELECT count(*)
FROM VALUES (named_struct('b', map(1, 2, 2, 3))), (named_struct('b', map(1, 3))), (named_struct('b', map(2, 3, 1, 2))) as t(a)
GROUP BY a.b;
