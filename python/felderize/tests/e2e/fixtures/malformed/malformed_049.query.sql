CREATE VIEW malformed_049_a AS
SELECT id, val FROM base WHERE val >= 8;

CREATE VIEW malformed_049 AS
SELECT id, val FROM malformed_049_a;
