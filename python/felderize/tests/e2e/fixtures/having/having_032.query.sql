CREATE VIEW having_032 AS
SELECT col1 AS alias
FROM values(1)
GROUP BY col1
HAVING (
    SELECT col1 = 1
);
