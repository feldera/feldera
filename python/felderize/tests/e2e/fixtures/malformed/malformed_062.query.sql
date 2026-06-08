CREATE VIEW malformed_062_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_062_b AS
SELECT id AS x, val AS x FROM malformed_062_a;

CREATE VIEW malformed_062 AS
SELECT x FROM malformed_062_b;
