CREATE VIEW string-functions_114 AS
select decode(scol, ecol) from values(X'68656c6c6f', 'Windows-xxx') as t(scol, ecol);
