CREATE VIEW string-functions_082 AS
SELECT hex(rpad(unhex('aa'), 5, unhex('1f2e')));
