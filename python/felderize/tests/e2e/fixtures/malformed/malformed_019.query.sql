CREATE VIEW malformed_019_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_019_b AS
SELECT id, nonexistent_col FROM malformed_019_a;

CREATE VIEW malformed_019 AS
SELECT id, nonexistent_col FROM malformed_019_b;
