CREATE VIEW malformed_045_a AS
SELECT id, val FROM base WHERE val >= 4;

CREATE VIEW malformed_045 AS
SELECT id, val FROM malformed_045_a;
