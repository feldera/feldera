CREATE VIEW string-functions_033 AS
SELECT split_part(str, delimiter, partNum) FROM VALUES ('11.12.13', '.', 3) AS v1(str, delimiter, partNum);
