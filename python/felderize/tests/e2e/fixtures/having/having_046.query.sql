CREATE VIEW having_046 AS
SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING bool_or(col2 = 1) AND bool_or(col2 = 1);
