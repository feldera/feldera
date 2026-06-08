CREATE VIEW malformed_063_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_063_b AS
SELECT id AS x, val AS x FROM malformed_063_a;

CREATE VIEW malformed_063 AS
SELECT x FROM malformed_063_b;
