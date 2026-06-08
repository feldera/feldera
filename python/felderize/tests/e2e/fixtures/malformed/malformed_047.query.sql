CREATE VIEW malformed_047_a AS
SELECT id, val FROM base WHERE val >= 6;

CREATE VIEW malformed_047 AS
SELECT id, val FROM malformed_047_a;
