CREATE VIEW malformed_038_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_038 AS
SELECT x.id, x.val FROM malformed_038_missing_7 x;
