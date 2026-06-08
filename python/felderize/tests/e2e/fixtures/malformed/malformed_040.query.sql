CREATE VIEW malformed_040_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_040 AS
SELECT x.id, x.val FROM malformed_040_missing_9 x;
