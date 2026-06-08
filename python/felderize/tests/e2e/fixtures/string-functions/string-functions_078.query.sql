CREATE VIEW string-functions_078 AS
SELECT hex(rpad(unhex(''), 5, unhex('1f')));
