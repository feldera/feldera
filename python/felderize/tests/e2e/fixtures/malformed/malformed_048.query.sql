CREATE VIEW malformed_048_a AS
SELECT id, val FROM base WHERE val >= 7;

CREATE VIEW malformed_048 AS
SELECT id, val FROM malformed_048_a;
