CREATE VIEW string-functions_080 AS
SELECT hex(rpad(unhex('aa'), 6, unhex('1f')));
