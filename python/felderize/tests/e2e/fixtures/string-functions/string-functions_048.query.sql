CREATE VIEW string-functions_048 AS
SELECT trim(BOTH 'x' FROM 'xxxbarxxx'), trim('x' FROM 'xxxbarxxx');
