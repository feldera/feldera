CREATE VIEW string-functions_071 AS
SELECT hex(lpad(unhex('aabbcc'), 6, unhex('')));
