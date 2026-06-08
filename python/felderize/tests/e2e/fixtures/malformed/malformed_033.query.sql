CREATE VIEW malformed_033_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_033 AS
SELECT x.id, x.val FROM malformed_033_missing_2 x;
