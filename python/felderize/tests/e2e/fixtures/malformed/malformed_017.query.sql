CREATE VIEW malformed_017_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_017_b AS
SELECT id, nonexistent_col FROM malformed_017_a;

CREATE VIEW malformed_017 AS
SELECT id, nonexistent_col FROM malformed_017_b;
