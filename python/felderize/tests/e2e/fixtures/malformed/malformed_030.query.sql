CREATE VIEW malformed_030_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_030_b AS
SELECT id, val FROM malformed_030_a WHERE val > 9;

CREATE VIEW malformed_030 AS
SELECT id, val FROM malformed_030_b;
