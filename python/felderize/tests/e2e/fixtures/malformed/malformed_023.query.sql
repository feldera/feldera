CREATE VIEW malformed_023_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_023_b AS
SELECT id, val FROM malformed_023_a WHERE val > 2;

CREATE VIEW malformed_023 AS
SELECT id, val FROM malformed_023_b;
