CREATE VIEW malformed_070_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_070_b AS
SELECT id AS x, val AS x FROM malformed_070_a;

CREATE VIEW malformed_070 AS
SELECT x FROM malformed_070_b;
