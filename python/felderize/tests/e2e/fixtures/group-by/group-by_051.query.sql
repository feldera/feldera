CREATE VIEW group-by_051 AS
SELECT count(*)
FROM VALUES (ARRAY(named_struct('b', map(1, 2, 2, 3)), named_struct('b', map(1, 3)))), (ARRAY(named_struct('b', map(2, 3)), named_struct('b', map(1, 3)))), (ARRAY(named_struct('b', map(2, 3, 1, 2)), named_struct('b', map(1, 3)))) as t(a)
group BY a;
