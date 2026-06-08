CREATE VIEW string-functions_092 AS
select encode(scol, ecol) from values('hello', 'WINDOWS-1252') as t(scol, ecol);
