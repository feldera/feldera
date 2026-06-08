CREATE VIEW group-by_029 AS
SELECT histogram_numeric(col, 3) FROM VALUES (1F), (2F), (3F) AS tab(col);
