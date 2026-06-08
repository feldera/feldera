CREATE VIEW string-functions_058 AS
SELECT btrim(encode('xxxbarxxx', 'utf-8'), encode('x', 'utf-8'));
