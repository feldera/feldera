CREATE VIEW string-functions_098 AS
select encode(scol, ecol) from values('渭城朝雨浥轻尘', 'US-ASCII') as t(scol, ecol);
