CREATE VIEW malformed_043_a AS
SELECT id, val FROM base WHERE val >= 2;

CREATE VIEW malformed_043 AS
SELECT id, val FROM malformed_043_a;
