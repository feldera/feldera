CREATE VIEW malformed_044_a AS
SELECT id, val FROM base WHERE val >= 3;

CREATE VIEW malformed_044 AS
SELECT id, val FROM malformed_044_a;
