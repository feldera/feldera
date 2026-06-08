CREATE VIEW malformed_028_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_028_b AS
SELECT id, val FROM malformed_028_a WHERE val > 7;

CREATE VIEW malformed_028 AS
SELECT id, val FROM malformed_028_b;
