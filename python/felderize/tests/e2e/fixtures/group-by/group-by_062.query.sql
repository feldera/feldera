CREATE VIEW group-by_062 AS
SELECT col1 FROM t1 GROUP BY ALL HAVING any_value(col2) = 'a';
