CREATE VIEW string-functions_066 AS
SELECT hex(lpad(unhex('aa'), 6, unhex('1f')));
