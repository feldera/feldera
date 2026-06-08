CREATE VIEW order-by_002 AS
SELECT col1 FROM VALUES (1, named_struct('f1', named_struct('f2', 1))) ORDER BY col2.f1.f2;
