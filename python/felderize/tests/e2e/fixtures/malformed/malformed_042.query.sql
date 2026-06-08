CREATE VIEW malformed_042_a AS
SELECT id, val FROM base WHERE val >= 1;

CREATE VIEW malformed_042 AS
SELECT id, val FROM malformed_042_a;
