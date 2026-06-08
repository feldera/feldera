CREATE VIEW malformed_061_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_061_b AS
SELECT id AS x, val AS x FROM malformed_061_a;

CREATE VIEW malformed_061 AS
SELECT x FROM malformed_061_b;
