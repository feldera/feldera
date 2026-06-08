CREATE VIEW order-by_009 AS
SELECT make_date(col1, col2, col3) AS a, a AS b FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) ORDER BY make_date(col1, col2, col3);
