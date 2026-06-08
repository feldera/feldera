CREATE VIEW malformed_031_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_031 AS
SELECT x.id, x.val FROM malformed_031_missing_0 x;
