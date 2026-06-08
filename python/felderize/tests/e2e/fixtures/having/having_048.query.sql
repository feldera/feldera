CREATE VIEW having_048 AS
SELECT col1 FROM VALUES (1) t1 GROUP BY col1 HAVING (
    SELECT MAX(t2.col1) FROM VALUES (1) t2 WHERE t2.col1 == MAX(t1.col1) GROUP BY t2.col1 HAVING (
        SELECT t3.col1 FROM VALUES (1) t3 WHERE t3.col1 == MAX(t2.col1)
    )
);
