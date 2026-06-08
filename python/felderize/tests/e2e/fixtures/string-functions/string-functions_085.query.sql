CREATE VIEW string-functions_085 AS
SELECT hex(rpad(unhex('aabbcc'), 6, unhex('')));
