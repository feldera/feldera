CREATE VIEW group-by_055 AS
SELECT count(*)
FROM VALUES (map(named_struct('b', 1), 2, named_struct('b', 2), 3)), (map(named_struct('b', 1), 3)), (map(named_struct('b', 2), 3, named_struct('b', 1), 2)) as t(a)
group BY a;
