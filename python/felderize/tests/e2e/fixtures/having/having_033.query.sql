CREATE VIEW having_033 AS
SELECT col1 AS alias
FROM values(named_struct('a', 1))
GROUP BY col1
HAVING (
    SELECT col1.a = 1
);
