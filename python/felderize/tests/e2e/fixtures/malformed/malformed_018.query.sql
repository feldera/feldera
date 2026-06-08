CREATE VIEW malformed_018_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_018_b AS
SELECT id, nonexistent_col FROM malformed_018_a;

CREATE VIEW malformed_018 AS
SELECT id, nonexistent_col FROM malformed_018_b;
