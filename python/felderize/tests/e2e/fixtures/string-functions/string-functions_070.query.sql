CREATE VIEW string-functions_070 AS
SELECT hex(lpad(unhex(''), 6, unhex('')));
