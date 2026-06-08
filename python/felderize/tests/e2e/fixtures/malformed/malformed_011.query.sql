CREATE VIEW malformed_011_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_011_b AS
SELECT id, nonexistent_col FROM malformed_011_a;

CREATE VIEW malformed_011 AS
SELECT id, nonexistent_col FROM malformed_011_b;
