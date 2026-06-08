CREATE VIEW malformed_066_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_066_b AS
SELECT id AS x, val AS x FROM malformed_066_a;

CREATE VIEW malformed_066 AS
SELECT x FROM malformed_066_b;
