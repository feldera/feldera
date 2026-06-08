CREATE VIEW string-functions_094 AS
select encode(scol, ecol) from values('hello', 'WINDOWS-1252') as t(scol, ecol);
