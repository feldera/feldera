CREATE VIEW malformed_067_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_067_b AS
SELECT id AS x, val AS x FROM malformed_067_a;

CREATE VIEW malformed_067 AS
SELECT x FROM malformed_067_b;
