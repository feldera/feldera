CREATE VIEW string-functions_096 AS
select encode(scol, ecol) from values('hello', 'Windows-xxx') as t(scol, ecol);
