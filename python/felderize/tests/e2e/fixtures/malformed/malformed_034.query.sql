CREATE VIEW malformed_034_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_034 AS
SELECT x.id, x.val FROM malformed_034_missing_3 x;
