CREATE VIEW string-functions_069 AS
SELECT hex(lpad(unhex('aa'), 6, unhex('1f2e')));
