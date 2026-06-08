CREATE VIEW malformed_020_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_020_b AS
SELECT id, nonexistent_col FROM malformed_020_a;

CREATE VIEW malformed_020 AS
SELECT id, nonexistent_col FROM malformed_020_b;
