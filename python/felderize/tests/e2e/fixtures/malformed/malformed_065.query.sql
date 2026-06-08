CREATE VIEW malformed_065_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_065_b AS
SELECT id AS x, val AS x FROM malformed_065_a;

CREATE VIEW malformed_065 AS
SELECT x FROM malformed_065_b;
