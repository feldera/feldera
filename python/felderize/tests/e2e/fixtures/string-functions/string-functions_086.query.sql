CREATE VIEW string-functions_086 AS
SELECT hex(rpad(unhex('aabbcc'), 2, unhex('ff')));
