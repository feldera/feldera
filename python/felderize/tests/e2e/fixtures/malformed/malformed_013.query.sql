CREATE VIEW malformed_013_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_013_b AS
SELECT id, nonexistent_col FROM malformed_013_a;

CREATE VIEW malformed_013 AS
SELECT id, nonexistent_col FROM malformed_013_b;
