CREATE VIEW string-functions_065 AS
SELECT hex(lpad(unhex('aa'), 5, unhex('1f')));
