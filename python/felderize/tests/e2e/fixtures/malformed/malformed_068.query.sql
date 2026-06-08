CREATE VIEW malformed_068_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_068_b AS
SELECT id AS x, val AS x FROM malformed_068_a;

CREATE VIEW malformed_068 AS
SELECT x FROM malformed_068_b;
