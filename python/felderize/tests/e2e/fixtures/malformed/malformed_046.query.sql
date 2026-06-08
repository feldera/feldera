CREATE VIEW malformed_046_a AS
SELECT id, val FROM base WHERE val >= 5;

CREATE VIEW malformed_046 AS
SELECT id, val FROM malformed_046_a;
