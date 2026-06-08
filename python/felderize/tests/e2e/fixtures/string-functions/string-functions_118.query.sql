CREATE VIEW string-functions_118 AS
select decode(scol, ecol) from values(X'68656c6c6f', 'WINDOWS-1252') as t(scol, ecol);
