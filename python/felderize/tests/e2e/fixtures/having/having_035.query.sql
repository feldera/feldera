CREATE VIEW having_035 AS
SELECT col1 AS alias
FROM values(map('a', 1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);
