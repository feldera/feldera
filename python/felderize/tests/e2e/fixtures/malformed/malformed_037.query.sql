CREATE VIEW malformed_037_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_037 AS
SELECT x.id, x.val FROM malformed_037_missing_6 x;
