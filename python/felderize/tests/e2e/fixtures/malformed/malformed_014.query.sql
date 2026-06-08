CREATE VIEW malformed_014_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_014_b AS
SELECT id, nonexistent_col FROM malformed_014_a;

CREATE VIEW malformed_014 AS
SELECT id, nonexistent_col FROM malformed_014_b;
