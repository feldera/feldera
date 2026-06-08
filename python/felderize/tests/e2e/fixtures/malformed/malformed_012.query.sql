CREATE VIEW malformed_012_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_012_b AS
SELECT id, nonexistent_col FROM malformed_012_a;

CREATE VIEW malformed_012 AS
SELECT id, nonexistent_col FROM malformed_012_b;
