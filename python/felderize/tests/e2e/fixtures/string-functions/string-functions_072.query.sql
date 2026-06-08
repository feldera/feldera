CREATE VIEW string-functions_072 AS
SELECT hex(lpad(unhex('aabbcc'), 2, unhex('ff')));
