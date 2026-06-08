CREATE VIEW having_034 AS
SELECT col1 AS alias
FROM values(array(1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);
