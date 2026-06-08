CREATE VIEW malformed_039_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_039 AS
SELECT x.id, x.val FROM malformed_039_missing_8 x;
