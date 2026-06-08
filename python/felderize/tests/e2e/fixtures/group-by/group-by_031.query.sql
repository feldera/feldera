CREATE VIEW group-by_031 AS
SELECT histogram_numeric(col, 3) FROM VALUES (1S), (2S), (3S) AS tab(col);
