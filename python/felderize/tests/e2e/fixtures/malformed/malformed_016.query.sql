CREATE VIEW malformed_016_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_016_b AS
SELECT id, nonexistent_col FROM malformed_016_a;

CREATE VIEW malformed_016 AS
SELECT id, nonexistent_col FROM malformed_016_b;
