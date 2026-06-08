CREATE VIEW group-by_050 AS
SELECT count(*)
FROM VALUES (named_struct('b', array(map(1, 2, 2, 3), map(1, 3)))), (named_struct('b', array(map(2, 3), map(1, 3)))), (named_struct('b', array(map(2, 3, 1, 2), map(1, 3)))) as t(a)
GROUP BY a;
