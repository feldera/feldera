CREATE VIEW malformed_069_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_069_b AS
SELECT id AS x, val AS x FROM malformed_069_a;

CREATE VIEW malformed_069 AS
SELECT x FROM malformed_069_b;
