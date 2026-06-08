CREATE VIEW group-by_027 AS
SELECT histogram_numeric(col, 3) FROM VALUES (1), (2), (3) AS tab(col);
