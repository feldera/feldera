CREATE VIEW string-functions_008 AS
select right("abcd", 2), right("abcd", 5), right("abcd", '2'), right("abcd", null);
