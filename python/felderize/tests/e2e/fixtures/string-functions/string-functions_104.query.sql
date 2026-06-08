CREATE VIEW string-functions_104 AS
select decode(encode('abc', 'utf-8'), 'utf-8');
