CREATE VIEW group-by_057 AS
SELECT count(*)
FROM VALUES (named_struct('b', map(named_struct('c', 1), 2, named_struct('c', 2), 3))), (named_struct('b', map(named_struct('c', 1), 3))), (named_struct('b', map(named_struct('c', 2), 3, named_struct('c', 1), 2))) as t(a)
group BY a.b;
