CREATE VIEW order-by_001 AS
SELECT col1 FROM VALUES (1, named_struct('f1', 1)) ORDER BY col2.f1;
