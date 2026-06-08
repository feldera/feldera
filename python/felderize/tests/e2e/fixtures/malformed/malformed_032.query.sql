CREATE VIEW malformed_032_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_032 AS
SELECT x.id, x.val FROM malformed_032_missing_1 x;
