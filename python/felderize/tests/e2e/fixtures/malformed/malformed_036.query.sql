CREATE VIEW malformed_036_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_036 AS
SELECT x.id, x.val FROM malformed_036_missing_5 x;
