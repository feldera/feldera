CREATE VIEW string-functions_006 AS
select left("abcd", 2), left("abcd", 5), left("abcd", '2'), left("abcd", null);
