CREATE VIEW string-functions_081 AS
SELECT hex(rpad(unhex(''), 5, unhex('1f2e')));
