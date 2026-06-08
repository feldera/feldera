CREATE VIEW malformed_064_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_064_b AS
SELECT id AS x, val AS x FROM malformed_064_a;

CREATE VIEW malformed_064 AS
SELECT x FROM malformed_064_b;
