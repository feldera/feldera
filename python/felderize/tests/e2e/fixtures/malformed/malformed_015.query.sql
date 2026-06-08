CREATE VIEW malformed_015_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_015_b AS
SELECT id, nonexistent_col FROM malformed_015_a;

CREATE VIEW malformed_015 AS
SELECT id, nonexistent_col FROM malformed_015_b;
