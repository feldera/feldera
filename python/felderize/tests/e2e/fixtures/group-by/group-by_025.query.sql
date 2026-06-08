CREATE VIEW group-by_025 AS
SELECT AVG(DISTINCT decimal_col), SUM(DISTINCT decimal_col) FROM VALUES (CAST(1 AS DECIMAL(9, 0))) t(decimal_col);
